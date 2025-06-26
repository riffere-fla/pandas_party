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
        DROP TABLE IF EXISTS custom.ctwm_v_tv;

        CREATE TABLE custom.ctwm_v_tv AS (

                        WITH event_types AS (
                SELECT DISTINCT
                    event_type
                FROM
                    custom.ctwm_event_descriptions
            ),
            event_dates AS (
                SELECT TOP 5 DISTINCT
                    event_datetime,
                    event_name,
                    primary_event_type,
                    secondary_event_type,
                    start_time_tableau,
                    GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS current_time_est
                FROM
                    custom.ctwm_event_descriptions
                WHERE
                    event_datetime::date >= current_time_est::date
                    AND event_type != 'War Memorial 3PE Premium '
                ORDER BY
                    event_datetime
            ),
            seatgeek_id AS (
                SELECT
                    event_datetime,
                    event_type,
                    seatgeek_product_id
                FROM
                    custom.ctwm_event_descriptions
            ),
            cross_table AS (
                SELECT
                    event_types.event_type,
                    event_dates.event_datetime,
                    event_dates.event_name,
                    event_dates.primary_event_type,
                    event_dates.secondary_event_type,
                    event_dates.start_time_tableau,
                    seatgeek_id.seatgeek_product_id
                FROM
                    event_types
                CROSS JOIN
                    event_dates
                LEFT JOIN
                    seatgeek_id
                        ON event_dates.event_datetime = seatgeek_id.event_datetime
                        AND event_types.event_type = seatgeek_id.event_type
            ),
            tickets_temp AS (
                SELECT
                    event_datetime,
                    REPLACE(REPLACE(event_type, ' FELD', ''), ' CONCERT', '') AS event_types,
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
                    custom.ctwm_v_ticket_2526 ticket
                INNER JOIN
                    event_dates USING (event_datetime)
            ),
            tickets AS (
                SELECT
                    event_datetime,
                    event_types,
                    sum(total_seats) AS "total_seats",
                    sum(todays_tickets) AS "todays_tickets",
                    sum(yesterdays_tickets) AS "yesterdays_tickets",
                    sum(last_hours_tickets) AS "last_hours_tickets"
                FROM
                    tickets_temp
                GROUP BY
                    event_datetime, event_types
            ),
            ticket_status AS (
                SELECT
                    event_datetime,
                    product_id as seatgeek_product_id,
                    listagg(distinct locks),
                    listagg(distinct allocations),
                    count(*) as capacity,
                    count(CASE WHEN status IN ('SOLD', 'HELD') THEN 1 END)::float / count(*) AS sell_through
                FROM
                    custom.ctwm_v_ticket_status
                INNER JOIN
                    event_dates USING (event_datetime)
                WHERE
                    (lower(locks) NOT ILIKE '%kill%' or locks is null)
                    AND (lower(allocations) NOT ILIKE '%kill%' or allocations is null)
                GROUP BY
                    event_datetime,
                    product_id
            )
            SELECT
                cross_table.event_type,
                cross_table.event_datetime,
                cross_table.seatgeek_product_id,
                cross_table.event_name,
                cross_table.secondary_event_type,
                cross_table.start_time_tableau,
                coalesce(ticket_status.capacity, 0) AS "capacity",
                coalesce(ticket_status.sell_through, 0) AS "sell_through",
                coalesce(tickets.total_seats, 0) AS total_seats,
                coalesce(tickets.todays_tickets, 0) AS todays_tickets,
                coalesce(tickets.yesterdays_tickets, 0) AS yesterdays_tickets,
                coalesce(tickets.last_hours_tickets, 0) AS last_hours_tickets,
                CONVERT_TIMEZONE('UTC', 'America/New_York', getdate()) AS "processed_date"
            FROM
                cross_table
            LEFT JOIN
                tickets
                    ON cross_table.event_datetime = tickets.event_datetime
                    AND cross_table.event_type = tickets.event_types
            LEFT JOIN
                ticket_status ON cross_table.seatgeek_product_id = ticket_status.seatgeek_product_id
            ORDER BY
                cross_table.event_datetime, 
                cross_table.event_type
        
        );
    """
    FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None 

@task(log_prints = True, retries=3, retry_delay_seconds=[10,10,10])
def create_table_hourly(redshift_credentials: Dict) -> None:

    q = """
        DROP TABLE IF EXISTS custom.ctwm_v_tv_hourly;

        CREATE TABLE custom.ctwm_v_tv_hourly AS (

            WITH all_sales AS (
                SELECT
                    date_trunc('hour', transaction_date)::timestamp AS "sales_hour",
                    CASE
                        WHEN ticket_type in ('Bar Stools', 'Box Seats','Suites') THEN 'Premium'
                        ELSE 'Other'
                    END AS "premium_vs_bowl",
                    sum(paid_seats) AS "paid_seats"
                FROM
                    custom.ctwm_v_ticket_2526
                WHERE
                    sales_hour::date >= ((getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') - INTERVAL '1 day')::date
                    AND sales_hour::date <= (getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date
                GROUP BY
                    sales_hour, premium_vs_bowl
            ),
            all_hours AS (
                SELECT
                    dateadd(hour, a.hour_num::int, b.sales_hour::timestamp) AS "sales_hour",
                    c.premium_vs_bowl
                FROM
                    (SELECT DISTINCT date_part(HOUR, transaction_date) AS "hour_num" FROM custom.ctwm_v_ticket_2526) a
                CROSS JOIN
                    (SELECT DISTINCT sales_hour::date FROM all_sales) b
                CROSS JOIN
                    (SELECT DISTINCT premium_vs_bowl FROM all_sales) c
            )
            SELECT
                all_hours.sales_hour,
                all_hours.premium_vs_bowl,
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
                    AND all_hours.premium_vs_bowl = all_sales.premium_vs_bowl
            ORDER BY
                sales_hour, premium_vs_bowl
        
        );
    """
    FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None 

########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def cte_v_tv() -> None:

    redshift_credentials = get_redshift_credentials()

    create_table(redshift_credentials)
    create_table_hourly(redshift_credentials)

    return None 


if __name__ == "__main__":

    cte_v_tv()