from datetime import datetime

from utils.get_dag_id import get_dag_id

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define the default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

# Instantiate the DAG
dag = DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="Create views",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

# SQL statements
sql_statements = """

CREATE OR REPLACE
VIEW v_user_acquisition_channels AS
SELECT
	u.id
	, CASE
		WHEN urr.ref_type IS NULL THEN 'Organic'::TEXT
		WHEN urr.ref_type = 'PERSONAL' AND urc.campaign = 'airdrop' THEN 'Airdrop'
		ELSE initcap(
			urr.ref_type::TEXT
		)
	END AS ref_type
	, u.created_at
FROM
	warehouse_staging."user" u
LEFT JOIN warehouse_staging.user_ref_registration urr ON
	u.id = urr.user_id
LEFT JOIN warehouse_staging.user_registration_campaign urc ON
	u.id = urc.user_id
WHERE
	u.deleted_at IS NULL
	AND u.is_deactivated IS NOT TRUE;
 

CREATE OR REPLACE
VIEW v_user_acquisition_channels_agg_by_date AS
SELECT
	date(created_at) AS created_at
	, count(*)
	, CASE
		WHEN ref_type IS NULL THEN 'Total'
		ELSE ref_type
	END AS ref_type
FROM
	v_user_acquisition_channels
GROUP BY
	CUBE (
		ref_type
		, date(created_at)
	)
HAVING
	date(created_at) IS NOT NULL
ORDER BY
	created_at DESC
	, ref_type;


CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_nfts AS WITH nft_counts AS (
    SELECT
        w.user_id
        , sum( CASE WHEN n.type::TEXT = 'bcnft'::TEXT THEN 1 ELSE 0 END) AS bcnft
        , sum( CASE WHEN n.type::TEXT = 'bpnft'::TEXT THEN 1 ELSE 0 END) AS bpnft
        , sum( CASE WHEN n.type::TEXT = 'bunft'::TEXT THEN 1 ELSE 0 END) AS bunft
        , sum( CASE WHEN n.type::TEXT = 'ocnft'::TEXT THEN 1 ELSE 0 END) AS ocnft
        , sum( CASE WHEN n.type::TEXT = 'opnft'::TEXT THEN 1 ELSE 0 END) AS opnft
        , sum( CASE WHEN n.type::TEXT = 'ounft'::TEXT THEN 1 ELSE 0 END) AS ounft
    FROM
        warehouse_staging.nft n
    JOIN warehouse_staging.wallet w ON
        w.account_address::TEXT = n.owner_address::TEXT
    GROUP BY
        w.user_id
)
SELECT
    nft_counts.user_id
    , nft_counts.bcnft
    , nft_counts.bpnft
    , nft_counts.bunft
    , nft_counts.ocnft
    , nft_counts.opnft
    , nft_counts.ounft
    , nft_counts.bcnft + nft_counts.bpnft + nft_counts.bunft + nft_counts.ocnft + nft_counts.opnft + nft_counts.ounft AS total_nfts
FROM
    nft_counts;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_medals AS
SELECT
    ur.user_id
    , sum(ur.point) AS total_medals
FROM
    warehouse_staging.user_reward ur
GROUP BY
    ur.user_id;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_own_communities AS
SELECT
    c.owner_id AS user_id
    , count(*) AS total_own_communities
FROM
    warehouse_staging.community c
WHERE
    c.deleted_at IS NULL
GROUP BY
    c.owner_id;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_refs AS
SELECT
    user_personal_ref.user_id
    , user_personal_ref.total_personal_refs + user_own_com_ref.total_own_com_refs AS total_refs
FROM
    (
        SELECT
            u.id AS user_id
            , count(*) AS total_personal_refs
        FROM
            warehouse_staging.user_ref_registration urr
        JOIN warehouse_staging."user" u ON
            u.referral_code::TEXT = urr.ref_code::TEXT
        GROUP BY
            u.id
    ) user_personal_ref
JOIN (
        SELECT
            c.owner_id
            , count(*) AS total_own_com_refs
        FROM
            warehouse_staging.user_ref_registration urr
        JOIN warehouse_staging."group" g ON
            urr.ref_code::TEXT = g.referral_code::TEXT
        JOIN warehouse_staging.community c ON
            c.group_id = g.id
        JOIN warehouse_staging."user" u ON
            u.id = c.owner_id
        GROUP BY
            c.owner_id
    ) user_own_com_ref ON
    user_own_com_ref.owner_id = user_personal_ref.user_id;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_joined_communities AS
SELECT
    cm.user_id
    , count(*) AS total_joined_communities
FROM
    warehouse_staging.community_member cm
WHERE
    cm.deleted_at IS NULL
    AND NOT (
        EXISTS (
            SELECT
                c.id
            FROM
                warehouse_staging.community c
            WHERE
                c.owner_id = cm.user_id
                AND c.id = cm.community_id
        )
    )
GROUP BY
    cm.user_id;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_contents AS
SELECT
    p.created_by AS user_id
    , count(*) AS total_contents
FROM
    warehouse_staging.post p
WHERE
    p.status::TEXT = 'PUBLISHED'::TEXT
    AND p.is_hidden IS NOT TRUE
GROUP BY
    p.created_by;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_comments AS
SELECT
    comment.created_by AS user_id
    , count(*) AS total_comments
FROM
    warehouse_staging.comment
WHERE
    comment.is_hidden IS NOT TRUE
GROUP BY
    comment.created_by;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_reactions AS
SELECT
    sub.created_by AS user_id
    , count(*) AS total_reactions
FROM
    (
        SELECT
            pr.created_by
            , pr.id
        FROM
            warehouse_staging.post_reaction pr
    UNION
        SELECT
            cr.created_by
            , cr.id
        FROM
            warehouse_staging.comment_reaction cr
    ) sub
GROUP BY
    sub.created_by
ORDER BY
    sub.created_by;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_save_posts AS
SELECT
    usp.user_id
    , count(*) AS total_save_posts
FROM
    warehouse_staging.user_save_post usp
GROUP BY
    usp.user_id;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_total_seen_posts AS
SELECT
    usp.user_id
    , count(*) AS total_seen_posts
FROM
    warehouse_staging.user_seen_post usp
GROUP BY
    usp.user_id;

CREATE OR REPLACE
VIEW warehouse_staging.v_user_overview AS
SELECT
    u.username
    , ue.status AS kyc_status
    , v_user_total_nfts.total_nfts
    , v_user_total_medals.total_medals
    , v_user_total_refs.total_refs
    , v_user_total_own_communities.total_own_communities
    , v_user_total_joined_communities.total_joined_communities
    , v_user_total_contents.total_contents
    , v_user_total_comments.total_comments
    , v_user_total_reactions.total_reactions
    , v_user_total_save_posts.total_save_posts
    , v_user_total_seen_posts.total_seen_posts
FROM
    warehouse_staging."user" u
LEFT JOIN warehouse_staging.user_ekyc ue ON
    u.id = ue.user_id
LEFT JOIN warehouse_staging.v_user_total_nfts ON
    u.id = v_user_total_nfts.user_id
LEFT JOIN warehouse_staging.v_user_total_medals ON
    u.id = v_user_total_medals.user_id
LEFT JOIN warehouse_staging.v_user_total_own_communities ON
    u.id = v_user_total_own_communities.user_id
LEFT JOIN warehouse_staging.v_user_total_refs ON
    u.id = v_user_total_refs.user_id
LEFT JOIN warehouse_staging.v_user_total_joined_communities ON
    u.id = v_user_total_joined_communities.user_id
LEFT JOIN warehouse_staging.v_user_total_contents ON
    u.id = v_user_total_contents.user_id
LEFT JOIN warehouse_staging.v_user_total_participated_quizzes ON
    u.id = v_user_total_participated_quizzes.user_id
LEFT JOIN warehouse_staging.v_user_total_comments ON
    u.id = v_user_total_comments.user_id
LEFT JOIN warehouse_staging.v_user_total_reactions ON
    u.id = v_user_total_reactions.user_id
LEFT JOIN warehouse_staging.v_user_total_save_posts ON
    u.id = v_user_total_save_posts.user_id
LEFT JOIN warehouse_staging.v_user_total_seen_posts ON
    u.id = v_user_total_seen_posts.user_id;
"""

create_views_task = PostgresOperator(
    task_id="create_views",
    postgres_conn_id="warehouse_db_id",
    sql=sql_statements,
    dag=dag,
)
