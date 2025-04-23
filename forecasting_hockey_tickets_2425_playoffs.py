import pandas as pd

from prefect import flow, task
from prefect.blocks.system import Secret

from catnip.fla_redshift import FLA_Redshift

from typing import Dict

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

########################################################################
### SCHEMAS ############################################################
########################################################################

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
def extract_historical_ticket_sales(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH playoffs_22_23 AS (
            SELECT
                '2022-23' AS season,
                LEFT(RIGHT(event_name, 4), 2) AS round,
                event_name,
                date(event_date) as event_date,
                CASE
                    WHEN DATEDIFF('days', DATE(add_datetime), DATE(event_date)) >= 0
                        THEN DATEDIFF('days', DATE(add_datetime), DATE(event_date))
                    ELSE 0
                END AS days_out,
                SUM(block_purchase_price) AS gross_revenue,
                SUM(paid_seats) AS paid_seats
            FROM
                custom.cth_ticket_expanded_all_playoffs_2223
            WHERE
                event_name IN ('23POR1G1', '23POR1G2', '23POR1G3', '23POR2G1', '23POR2G2', '23POR3G1', '23POR3G2', '23POR4G1', '23POR4G2')
                AND ticket_type IN ('Singles')
            GROUP BY
                event_name,
                event_date,
                days_out,
                ticket_type
        ),
        playoffs_23_24 AS (
            SELECT
                '2023-24' AS season,
                RIGHT(LEFT(product_description, 6), 2) AS round,
                LEFT(product_description, 8) AS event_name,
                date(event_datetime) as event_date,
                CASE
                    WHEN DATEDIFF('days', DATE(transaction_date), DATE(event_datetime)) >= 0
                        THEN DATEDIFF('days', DATE(transaction_date), DATE(event_datetime))
                    ELSE 0
                END AS days_out,
                SUM(gross_revenue) AS gross_revenue,
                SUM(paid_seats) AS paid_seats
            FROM
                custom.cth_v_ticket_2324_playoffs
            WHERE
                ticket_type_playoffs IN ('Singles')
                AND event_name != '23-24 Pl'
            GROUP BY
                product_description,
                event_date,
                days_out,
                ticket_type_playoffs
        )
        SELECT
            *
        FROM
            playoffs_22_23
        UNION ALL
        SELECT
            *
        FROM
            playoffs_23_24
        ORDER BY
            season,
            round,
            event_name,
            days_out DESC
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def extract_upcoming_games(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        SELECT
            '2024-25' AS season,
            RIGHT(LEFT(product_description, 6), 2) AS round,
            LEFT(product_description, 8) AS event_name,
            date(event_datetime) as event_date,
            CASE
                WHEN DATEDIFF('days', DATE(transaction_date), DATE(event_datetime)) >= 0
                    THEN DATEDIFF('days', DATE(transaction_date), DATE(event_datetime))
                ELSE 0
            END AS days_out,
            SUM(gross_revenue) AS gross_revenue,
            SUM(paid_seats) AS paid_seats
        FROM
            custom.cth_v_ticket_2425_playoffs
        WHERE
            ticket_type_playoffs IN ('Singles')
            AND event_name != '24-25 Pl'
        GROUP BY
            product_description,
            event_date,
            days_out,
            ticket_type_playoffs
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)


@task(log_prints = True)
def transform_ticket_sales(df: pd.DataFrame) -> pd.DataFrame:

    df['min_days_out'] = df.apply(lambda row: df[(df['event_name'] == row['event_name'])]['days_out'].min(), axis = 1)

    cumdf = df.groupby(by = ['round','event_name','event_date'], axis = 0)[['gross_revenue','paid_seats']].cumsum().rename(
                                                    columns = {'gross_revenue':'cum_gross_rev', 'paid_seats':'cum_num_seats'})

    df = pd.concat([df,cumdf], axis = 1)

    df['final_seats'] = df.apply(lambda row: df[(df['event_name'] == row['event_name'])&
                                                    (df['days_out'] == row['min_days_out'])]['cum_num_seats'].item(), axis = 1)

    df['per_seats_in'] = [x/y for x,y in zip(df['cum_num_seats'],df['final_seats'])]

    df['final_rev'] = df.apply(lambda row: df[(df['event_name'] == row['event_name'])&
                                                    (df['days_out'] == row['min_days_out'])]['cum_gross_rev'].item(), axis = 1)

    df['per_rev_in'] = [x/y for x,y in zip(df['cum_gross_rev'],df['final_rev'])]

    df = df[['season','round', 'event_name', 'event_date','days_out','gross_revenue','paid_seats',
              'cum_gross_rev','cum_num_seats','per_seats_in','per_rev_in']]

    return df

@task(log_prints = True)
def get_seat_predictions(df_historical: pd.DataFrame, df_upcoming: pd.DataFrame) -> pd.DataFrame:

    # no suites

    df_avgs = df_historical.groupby(by = ['round','days_out'])[['per_seats_in','per_rev_in']].mean().rename(
        columns = {'per_seats_in':'avg_per_seats_in','per_rev_in':'avg_per_rev_in'}).reset_index()

    df_merged = df_upcoming.merge(right = df_avgs, how = 'left', on = ['round','days_out'])

    df_merged['paid_seats'] = df_merged['cum_num_seats']/df_merged['avg_per_seats_in']

    df_merged['gross_revenue'] = df_merged['cum_gross_rev']/df_merged['avg_per_rev_in']

    min_indices = df_merged.groupby('event_name')['days_out'].idxmin()

    result = df_merged.loc[min_indices]

    result['ticket_type_playoffs'] = 'Singles'

    result['tier'] = result['event_name'].str[-4:].str[:2]

    result = result[['event_name','event_date','tier','ticket_type_playoffs','paid_seats','gross_revenue']]

    return result


@task(log_prints=True)
def transform(redshift_credentials: Dict, df: pd.DataFrame) -> pd.DataFrame:

    q = """
        SELECT
            LEFT(product_description, 8) AS event_name,
            RIGHT(LEFT(product_description,6),2) AS tier,
            date(event_datetime) as event_date,
            ticket_type_playoffs,
            sum(gross_revenue) as gross_revenue,
            sum(paid_seats) as paid_seats
        FROM
            custom.cth_v_ticket_2425_playoffs
        WHERE
            date(event_datetime) <= '2025-05-02'
            and ticket_type_playoffs != 'Singles'
        GROUP BY
            product_description,
            event_date,
            ticket_type_playoffs
    """

    current_in = FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

    df = pd.concat([df,current_in])

    df = df.groupby(by = ['event_name','event_date', 'tier'])[['paid_seats','gross_revenue']].sum().reset_index()

    return df 


@task(log_prints = True)
def load(redshift_credentials: Dict, df: pd.DataFrame) -> None:

    FLA_Redshift(**redshift_credentials).write_to_warehouse(
        df = df,
        table_name = "forecasting_hockey_tickets_2425_playoffs_staging"
    )
    
    return None 


@task(log_prints = True)
def create_table(redshift_credentials: Dict) -> None:

    # add additional secondary and predict revenue
    q = """
        DROP TABLE IF EXISTS custom.forecasting_hockey_tickets_2425_playoffs;

        CREATE TABLE custom.forecasting_hockey_tickets_2425_playoffs AS (

            SELECT
                *
            FROM
                custom.forecasting_hockey_tickets_2425_playoffs_staging
        );
    """
    FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None 


########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def forecasting_hockey_tickets_2425_playoffs() -> None:

    # base credentials 
    redshift_credentials = get_redshift_credentials()

    # get historical data
    df_historical = extract_historical_ticket_sales(redshift_credentials)
    df_historical = transform_ticket_sales(df_historical)
    print(df_historical)

    # get upcoming games
    df_upcoming = extract_upcoming_games(redshift_credentials)
    df_upcoming = transform_ticket_sales(df_upcoming)
    print(df_upcoming)

    # predict seats
    df = get_seat_predictions(df_historical=df_historical, df_upcoming=df_upcoming)
    print(df)

    # final transform
    df = transform(redshift_credentials, df)
    print(df)

    # load staging
    load(redshift_credentials, df)

    # create final table w/ predicted revenue
    create_table(redshift_credentials)

    return None 


if __name__ == "__main__":

    forecasting_hockey_tickets_2425_playoffs()