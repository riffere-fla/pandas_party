import pandas as pd

from prefect import flow, task
from prefect.blocks.system import Secret

from catnip.fla_redshift import FLA_Redshift

from typing import Dict

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

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
def create_table(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH verticals AS (
            SELECT
                rep_name,
                CASE
                    WHEN rep_title ILIKE '%C.A.T.S.%' THEN 'CATS'
                    WHEN rep_department = 'Premium' AND rep_sales_service = 'Sales' THEN 'Premium Sales'
                    WHEN rep_department = 'Premium' AND rep_sales_service = 'Service' THEN 'Premium Service'
                    WHEN rep_department = 'Groups' THEN 'Groups'
                    WHEN rep_department = 'New Business' AND rep_sales_service = 'Sales' THEN 'New Biz'
                    WHEN rep_department = 'Membership' AND rep_sales_service = 'Service' THEN 'Service'
                    WHEN rep_name = 'Jack Ballou' THEN 'Jack Ballou'
                    WHEN rep_name = 'Catalina Cano' THEN 'Catalina Cano'
                    ELSE ''
                END AS vertical,
                CASE
                    WHEN vertical = 'Premium Sales' THEN 50
                    WHEN vertical = 'Premium Service' THEN 70
                    WHEN vertical = 'Groups' THEN 40
                    WHEN vertical = 'New Biz' THEN 40
                    WHEN vertical = 'Service' THEN 60
                    WHEN vertical = 'CATS' THEN 30
                    WHEN vertical = 'Jack Ballou' THEN 200
                    WHEN vertical = 'Catalina Cano' THEN 320
                    ELSE 0
                END AS goal
            FROM
                custom.korepss_v_users
            WHERE
                vertical != ''
                AND is_active = TRUE
                AND rep_name NOT IN (
                    'Christopher Bongo', 'Jillian Walker', 'Ashley Bishop',
                    'Austin Gray', 'Marina Bray'
                )
            ),
            base_table AS (
                SELECT
                    *
                FROM
                    verticals
                CROSS JOIN (
                    SELECT DISTINCT
                        event_datetime
                    FROM
                        custom.cte_event_descriptions
                    WHERE
                        event_type = '3PE Premium'
                )
            ),
            ticket_info AS (
                SELECT
                    event_datetime,
                    DATE(transaction_date) AS transaction_date,
                    sales_rep,
                    SUM(paid_seats) AS paid_seats,
                    SUM(gross_revenue) AS gross_revenue
                FROM
                    custom.cte_v_ticket_2425
                WHERE
                    event_type = '3PE Premium'
                    AND ticket_type != 'Suites'
                GROUP BY
                    event_datetime,
                    sales_rep,
                    DATE(transaction_date)
                UNION ALL
                SELECT
                    event_datetime,
                    DATE(transaction_date) AS transaction_date,
                    sales_rep,
                    SUM(paid_seats) AS paid_seats,
                    SUM(gross_revenue) AS gross_revenue
                FROM
                    custom.cte_v_ticket_2526
                WHERE
                    event_type = '3PE Premium'
                    AND ticket_type != 'Suites'
                GROUP BY
                    event_datetime,
                    sales_rep,
                    DATE(transaction_date)
            )
        SELECT
            base_table.event_datetime,
            base_table.vertical,
            base_table.rep_name,
            transaction_date,
            goal,
            COALESCE(paid_seats, 0) AS paid_seats,
            COALESCE(gross_revenue, 0) AS gross_revenue
        FROM
            base_table
        LEFT JOIN
            ticket_info
            ON base_table.event_datetime = ticket_info.event_datetime
            AND base_table.rep_name = ticket_info.sales_rep``
    """

    FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None

########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def goal_tracker_3pe() -> None:

    # base credentials 
    redshift_credentials = get_redshift_credentials()

    create_table(redshift_credentials)

    return None 


if __name__ == "__main__":

    goal_tracker_3pe()