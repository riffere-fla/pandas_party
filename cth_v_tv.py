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

@task(log_prints = True, retries=3, retry_delay_seconds=[10,10,10])
def create_table(redshift_credentials: Dict) -> None:

    q = """
        DROP TABLE IF EXISTS custom.cth_v_tv;

        CREATE TABLE custom.cth_v_tv AS (

            WITH next_5_event_dates AS (
                SELECT TOP 5
                    event_datetime::date AS "event_date",
                    GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS "current_time_est"
                FROM
                    custom.cth_game_descriptions
                WHERE
                    event_datetime >= current_time_est
                    AND event_datetime::date <> '2024-11-02'
                ORDER BY
                    event_date
            ),
            tickets AS (
                SELECT
                    event_datetime::date AS "event_date",
                    GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS current_time_est,
                    paid_seats + comp_seats AS "total_seats",
                    CASE
                        WHEN transaction_date::date = current_time_est::date THEN paid_seats
                        ELSE 0
                    END AS todays_tickets,
                    CASE
                        WHEN transaction_date::date = (current_time_est - INTERVAL '1 day')::date THEN paid_seats
                        ELSE 0
                    END AS yesterdays_tickets,
                    CASE
                        WHEN transaction_date >= (current_time_est - INTERVAL '1 hour') THEN paid_seats
                        ELSE 0
                    END AS last_hours_tickets
                FROM
                    custom.cth_v_ticket_2526 ticket
            ),
            ticket_revenue AS (
                SELECT
                    event_date::date,
                    sum(gross_revenue) AS "gross_revenue"
                FROM
                    custom.cth_ticket_sales_summary_2526
                GROUP BY
                    event_date
            ),
            tickets_sum AS (
                SELECT
                    event_date::date as "event_date",
                    sum(todays_tickets) AS todays_tickets,
                    sum(yesterdays_tickets) AS yesterdays_tickets,
                    sum(last_hours_tickets) AS last_hours_tickets,
                    sum(total_seats) AS total_seats
                FROM
                    tickets
                GROUP BY
                    event_date
            ),
            ticket_status AS (
                SELECT
                    event_date,
                    count(*) as capacity,
                    count(CASE WHEN status IN ('SOLD', 'HELD') THEN 1 END)::float / count(*) AS sell_through
                FROM
                    custom.cth_v_ticket_status_2526 v_status
                INNER JOIN
                    next_5_event_dates USING (event_date)
                GROUP BY
                    event_date
            )
            SELECT
                tickets_sum.event_date,
                game_desc.is_premier,
                game_desc.tier,
                game_desc.abbreviation,
                game_desc.day_of_week,
                game_desc.start_time_tableau,
                game_desc.budget_goal,
                ticket_revenue.gross_revenue,
                ticket_status.capacity,
                coalesce(ticket_status.sell_through, 0) AS "sell_through",
                coalesce(tickets_sum.total_seats, 0) AS "total_seats",
                coalesce(tickets_sum.todays_tickets, 0) AS "todays_tickets",
                coalesce(tickets_sum.yesterdays_tickets, 0) AS "yesterdays_tickets",
                coalesce(tickets_sum.last_hours_tickets, 0) AS "last_hours_tickets",
                CASE
                    WHEN next_5_event_dates.current_time_est IS NULL THEN FALSE
                    ELSE TRUE
                END AS "next_5_flag",
                CONVERT_TIMEZONE('UTC', 'America/New_York', getdate()) AS "processed_date"
            FROM
                tickets_sum
            LEFT JOIN
                ticket_revenue ON tickets_sum.event_date = ticket_revenue.event_date
            LEFT JOIN
                ticket_status ON tickets_sum.event_date = ticket_status.event_date
            LEFT JOIN
                custom.cth_game_descriptions game_desc ON tickets_sum.event_date = game_desc.event_datetime::date
            LEFT JOIN
                next_5_event_dates ON tickets_sum.event_date = next_5_event_dates.event_date
            ORDER BY
                event_date
        
        );
    """
    FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None 

@task(log_prints = True, retries=3, retry_delay_seconds=[10,10,10])
def create_table_hourly(redshift_credentials: Dict) -> None:

    q = """
        DROP TABLE IF EXISTS custom.cth_v_tv_hourly;

        CREATE TABLE custom.cth_v_tv_hourly AS (

            WITH secondary_sales AS (
                SELECT
                    'Secondary'::varchar AS "primary_vs_secondary",
                    date_trunc('hour', add_datetime)::timestamp AS "sales_hour",
                    count(*) AS "paid_seats"
                FROM
                    custom.logitix_v_hockey_sales sales
                WHERE
                    sales_hour::date >= ((getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') - INTERVAL '1 day')::date
                    AND sales_hour::date <= (getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date
                    AND season = '2025-26'
                GROUP BY
                    sales_hour, primary_vs_secondary
            ),
            primary_sales AS (
                SELECT
                    'Primary'::varchar AS "primary_vs_secondary",
                    date_trunc('hour', transaction_date)::timestamp AS "sales_hour",
                    sum(paid_seats) AS "paid_seats"
                FROM
                    custom.cth_v_ticket_2526
                WHERE
                    sales_hour::date >= ((getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') - INTERVAL '1 day')::date
                    AND sales_hour::date <= (getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date
                GROUP BY
                    sales_hour
            ),
            all_sales AS (
                SELECT
                    *
                FROM
                    primary_sales
                UNION ALL
                SELECT
                    *
                FROM
                    secondary_sales
            ),
            all_hours AS (
                SELECT
                    dateadd(hour, a.hour_num::int, b.sales_hour::timestamp) AS "sales_hour",
                    c.primary_vs_secondary
                FROM
                    (SELECT DISTINCT date_part(HOUR, transaction_date) AS "hour_num" FROM custom.cth_v_ticket_2526) a
                CROSS JOIN
                    (SELECT DISTINCT sales_hour::date FROM all_sales) b
                CROSS JOIN
                    (SELECT DISTINCT primary_vs_secondary FROM all_sales) c
            )
            SELECT
                all_hours.sales_hour,
                all_hours.primary_vs_secondary,
                CASE 
                    WHEN all_hours.sales_hour <= GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' THEN coalesce(all_sales.paid_seats, 0)
                    ELSE all_sales.paid_seats 
                END AS "paid_seats",
                convert_timezone('UTC', 'America/New_York', getdate()) AS "processed_date"
            FROM
                all_hours
            LEFT JOIN
                all_sales
                    ON all_hours.sales_hour = all_sales.sales_hour
                    AND all_hours.primary_vs_secondary = all_sales.primary_vs_secondary
            ORDER BY
                sales_hour, primary_vs_secondary
        
        );
    """
    FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None 

########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def cth_v_tv() -> None:

    redshift_credentials = get_redshift_credentials()

    create_table(redshift_credentials)
    create_table_hourly(redshift_credentials)

    return None 


if __name__ == "__main__":

    cth_v_tv()