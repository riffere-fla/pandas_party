from prefect import flow, task
from prefect.blocks.system import Secret

from catnip.fla_redshift import FLA_Redshift

from typing import Dict

########################################################################
### CREDENTIALS ########################################################
########################################################################

def get_redshift_credentials() -> Dict:

    credentials = {
        "dbname": Secret.load("stellar-redshift-db-name").get(),
        "host": Secret.load("stellar-redshift-host").get(),
        "port": 5439,
        "user": Secret.load("stellar-redshift-user-name").get(),
        "password": Secret.load("stellar-redshift-password").get(),

        "aws_access_key_id": Secret.load("fla-s3-aws-access-key-id-east-1").get(),
        "aws_secret_access_key": Secret.load("fla-s3-aws-secret-access-key-east-1").get(),
        "bucket": Secret.load("fla-s3-bucket-name-east-1").get(),
        "subdirectory": "us-east-1",

        "verbose": False
    }

    return credentials

########################################################################
### ETL ################################################################
########################################################################

@task(log_prints = True)
def create_table() -> None:

    q = """
        DROP TABLE IF EXISTS custom.forecasting_hockey_nightly_suites_agg_playoffs;

        CREATE TABLE custom.forecasting_hockey_nightly_suites_agg_playoffs AS (

        WITH sold_suites AS (

            WITH comp_temp AS (
                SELECT
                    product_id,
                    section,
                    product_id || '-' || section AS "id",
                    'COMP'::varchar AS "status"
                FROM
                    custom.cth_v_ticket_status_2425_playoffs
                WHERE
                    (pc_one IN ('U', 'V', 'W') OR section = 'House')
                    AND status = 'SOLD'
                GROUP BY
                    product_id,
                    section
                HAVING
                    sum(gross_revenue) = 0
            )

            -- sold suites
            SELECT
                product_id,
                section,
                product_id || '-' || section AS "id",
                'SOLD'::varchar AS "status"
            FROM
                custom.cth_v_ticket_status_2425_playoffs
            WHERE
                (pc_one IN ('U', 'V', 'W') OR section = 'House')
                AND status = 'SOLD'
            GROUP BY
                product_id,
                section
            HAVING
                sum(gross_revenue) > 0
            UNION ALL

            -- killed suites
            SELECT
                product_id,
                section,
                product_id || '-' || section AS "id",
                'SOLD'::varchar AS "status"
            FROM
                custom.cth_v_ticket_status_2425_playoffs
            WHERE
                (
                    pc_one IN ('U', 'V', 'W')
                    OR section = 'House'
                )
                AND (
                    allocations ilike '%kill%'
                    OR locks ilike '%kill%'
                    OR allocations ilike '%panthers players%'
                    OR allocations ilike '%owner%'
                    OR allocations ilike '%hockey operations%'
                    OR allocations ilike '%visiting team%'
                )
                AND "id" NOT IN (SELECT ct.id FROM comp_temp ct)
            GROUP BY
                product_id,
                section
            UNION ALL

            -- comp suites
            SELECT * FROM comp_temp
        ),
        held_suites AS (
            SELECT
                product_id,
                section,
                product_id || '-' || section AS "id",
                'HELD'::varchar AS "status"
            FROM
                custom.cth_v_ticket_status_2425_playoffs
            WHERE
                (pc_one IN ('U', 'V', 'W') OR section = 'House')
                AND status = 'HELD'
                AND "id" NOT IN (SELECT s.id FROM sold_suites s)
            GROUP BY
                product_id,
                section
        ),
        -- SELECT * FROM held_suites;
        available_suites AS (
            SELECT
                product_id,
                section,
                product_id || '-' || section AS "id",
                'AVAIL'::varchar AS "status"
            FROM
                custom.cth_v_ticket_status_2425_playoffs
            WHERE
                (pc_one IN ('U', 'V', 'W') OR section = 'House')
                AND status = 'AVAIL'
                AND "id" NOT IN (SELECT s.id FROM sold_suites s)
                AND "id" NOT IN (SELECT h.id FROM held_suites h)
                AND (allocations <> '["Standing Room Only"]' OR allocations IS NULL)
            GROUP BY
                product_id,
                section
        ),
        temp_union AS (
            SELECT * FROM sold_suites
            UNION ALL
            SELECT * FROM held_suites
            UNION ALL
            SELECT * FROM available_suites
        ),
        temp AS (
            SELECT
                split_part(products.product_description, ' - ', 1) AS "event_name",
                LEFT(RIGHT(event_name,4),2) AS tier,
                datediff('day', current_date, event_date) AS "days_out_from_event",
                COUNT(CASE WHEN status = 'COMP' THEN 1 END) AS num_comp,
                COUNT(CASE WHEN status = 'AVAIL' THEN 1 END) AS num_avail,
                COUNT(CASE WHEN status = 'SOLD' THEN 1 END) AS num_sold,
                COUNT(CASE WHEN status = 'HELD' THEN 1 END) AS num_held
            FROM
                temp_union
            LEFT JOIN
                custom.seatgeek_v_products products ON temp_union.product_id = products.product_id
            GROUP BY
                event_name,
                event_date)
        SELECT
            event_name,
            tier,
            days_out_from_event,
            num_comp,
            num_avail,
            num_sold,
            num_held,
            CASE
                WHEN num_avail <= cumulative_avg_sold THEN TRUE
                ELSE FALSE
            END AS will_sell_out
        FROM
            temp
        LEFT JOIN
            custom.forecasting_hockey_nightly_suites_playoffs USING (tier, days_out_from_event);
    """
    FLA_Redshift(**get_redshift_credentials()).execute_and_commit(sql_string=q)

    return None 

########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def forecasting_hockey_nightly_suites_agg_playoffs() -> None:

    create_table()

    return None 


if __name__ == "__main__":

    forecasting_hockey_nightly_suites_agg_playoffs()