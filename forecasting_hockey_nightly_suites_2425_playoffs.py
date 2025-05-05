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
        DROP TABLE IF EXISTS custom.forecasting_hockey_nightly_suites_playoffs;

        CREATE TABLE custom.forecasting_hockey_nightly_suites_playoffs AS (

            WITH base_table AS (
                SELECT DISTINCT
                    datediff('day', calendar.date::date, game_desc.event_date::date) AS "days_out_from_event",
                    tier.tier
                FROM
                    custom.calendar_base calendar
                CROSS JOIN
                    (SELECT DISTINCT event_date FROM custom.cth_game_descriptions WHERE season IN ('2022-23','2023-24','2024-25') AND game_type = 2) game_desc
                CROSS JOIN
                    (SELECT DISTINCT tier FROM custom.cth_game_descriptions WHERE season IN ('2022-23','2023-24','2024-25') AND game_type = 2) tier
                WHERE
                    "days_out_from_event" BETWEEN 0 AND 57
            ),
            all_games AS (
                SELECT
                    ticket.event_datetime::date,
                    game_desc.tier,
                    ticket.days_out_from_event,
                    ticket.section_name,
                    1 AS is_sold
                FROM
                    custom.cth_v_historical_ticket ticket
                LEFT JOIN
                    custom.cth_game_descriptions game_desc ON ticket.event_datetime = game_desc.event_datetime
                WHERE
                    ticket.price_level IN ('U', 'V', 'W', 'Y')
                    AND tier in ('R1','R2','R3','SC')
                    AND ticket.is_comp = FALSE
                    AND ticket.event_datetime < current_date
                GROUP BY
                    ticket.event_datetime,
                    game_desc.tier,
                    ticket.days_out_from_event,
                    ticket.section_name
            ),
            distinct_tier_games AS (
                SELECT
                    tier,
                    COUNT(DISTINCT event_datetime) AS "num_games"
                FROM
                    custom.cth_game_descriptions
                WHERE
                    season IN ('2022-23','2023-24', '2024-25')
                GROUP BY
                    tier
            ),
            agg_table AS (
                SELECT
                    tier,
                    days_out_from_event,
                    SUM(is_sold) AS "num_sold"
                FROM
                    all_games
                GROUP BY
                    tier,
                    days_out_from_event
            )
            SELECT
                base_table.tier,
                base_table.days_out_from_event,
                coalesce(num_sold, 0)::float / num_games AS "avg_sold",
                SUM(avg_sold) OVER (
                    PARTITION BY base_table.tier
                    ORDER BY base_table.days_out_from_event
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS "cumulative_avg_sold"
            FROM
                base_table
            LEFT JOIN agg_table
                ON base_table.tier = agg_table.tier
                AND base_table.days_out_from_event = agg_table.days_out_from_event
            LEFT JOIN
                distinct_tier_games ON base_table.tier = distinct_tier_games.tier
            ORDER BY
                tier,
                days_out_from_event
        );
    """
    FLA_Redshift(**get_redshift_credentials()).execute_and_commit(sql_string=q)

    return None 

########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def forecasting_hockey_nightly_suites_playoffs() -> None:

    create_table()

    return None 


if __name__ == "__main__":

    forecasting_hockey_nightly_suites_playoffs()