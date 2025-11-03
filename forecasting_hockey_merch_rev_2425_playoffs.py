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
### HELPER FUNCTIONS ###################################################
########################################################################

def run_model(df: pd.DataFrame, df_future: pd.DataFrame) -> np.ndarray:

    x_train = df[['attendance','weekend_num','start_time_num','tier_num']]
    y_train = df[['gross_revenue']]

    x_test = df_future[['predicted_turnstile','weekend_num','start_time_num','tier_num']]

    scalar = StandardScaler()
    poly_features = scalar.fit_transform(x_train)

    polynomial = LinearRegression().fit(poly_features, np.array(y_train).ravel())
    poly_features_2 = scalar.fit_transform(x_test)

    return polynomial.predict(poly_features_2).astype(int)

########################################################################
### ETL ################################################################
########################################################################

@task(log_prints = True)
def extract_historical(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH attendance AS
            (SELECT
                event_datetime,
                COUNT(*) AS attendance
            FROM
                custom.cth_v_attendance_2324_playoffs
            GROUP BY
                event_datetime)
        SELECT
            cth_game_descriptions.season,
            cth_game_descriptions.event_date,
            tier,
            day_of_week,
            start_time,
            attendance,
            SUM(gross_revenue) AS gross_revenue,
            SUM(qty) AS quantity,
            COUNT(distinct invoice_id) AS num_orders
        FROM
            custom.retailpro_v_invoice_items
        LEFT JOIN
            custom.cth_game_descriptions ON retailpro_v_invoice_items.event_date = cth_game_descriptions.event_date
        LEFT JOIN
            attendance ON retailpro_v_invoice_items.event_date = date(attendance.event_datetime)
        WHERE
            season IN ('2023-24','2024-25')
            AND tier IN ('R1','R2','R3','SC')
        GROUP BY
            cth_game_descriptions.season,
            cth_game_descriptions.event_date,
            tier,
            is_premier,
            original_six_plus_extra,
            day_of_week,
            start_time,
            attendance
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def extract_upcoming(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
    SELECT
        season,
        date(cth_game_descriptions.event_date) AS event_date,
        day_of_week,
        cth_game_descriptions.tier,
        start_time,
        predicted_turnstile
    FROM
        custom.cth_game_descriptions
    LEFT JOIN
        custom.forecasting_hockey_turnstile_2425_playoffs on cth_game_descriptions.event_date = forecasting_hockey_turnstile_2425_playoffs.event_date
    WHERE
        cth_game_descriptions.event_date >= current_date
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def transform(df: pd.DataFrame) -> pd.DataFrame:

    def create_start_time_num(df: pd.DataFrame) -> pd.DataFrame:
        # Define the dictionary with lists of start times
        lookup = {
            1: ['12:30 PM', '12:45 PM', '1:00 PM', '3:00 PM', '3:30 PM'],
            2: ['4:00 PM', '5:00 PM', '6:00 PM']
        }

        # Default to 0 for any start time not in the lookup
        df['start_time_num'] = 0
        for key, times in lookup.items():
            df.loc[df['start_time'].isin(times), 'start_time_num'] = key

        return df
    
    def create_tier_num(df: pd.DataFrame) -> pd.DataFrame:
        # lookup = {
        #     'SC': 4,
        #     'R3': 3,
        #     'R2': 2,
        #     'R1': 1
        # }

        # df['tier_num'] = 0
        # for key, value in lookup.items():
        #     df.loc[df['tier'] == key, 'tier_num'] = value

        df = pd.get_dummies(df, columns=['tier'], prefix = '', prefix_sep = '')

        return df
    
    def create_weekend_num(df: pd.DataFrame) -> pd.DataFrame:

        lookup = {
                'Fri': 1,
                'Sat': 1,
                'Sun': 1,
                'Mon': 0,
                'Tue': 0,
                'Wed': 0,
                'Thu': 0
        }

        for key, value in lookup.items():
            df.loc[df['day_of_week'] == key, 'weekend_num'] = value

        return df
    
    df = create_start_time_num(df)
    df = create_tier_num(df)
    df = create_weekend_num(df)
    
    return df

@task(log_prints = True)
def get_predictions(df_historical: pd.DataFrame, df_upcoming: pd.DataFrame) -> pd.DataFrame:

    df_historical['predicted_gross_revenue'] = df_historical['gross_revenue'].fillna(0)
    df_upcoming['predicted_gross_revenue'] = run_model(df_historical, df_upcoming)

    df = pd.concat([df_historical, df_upcoming], axis=0, ignore_index=True)

    df = df[df['season'] == '2024-25']
    df = df.rename(columns={'gross_revenue': 'current_gross_revenue'})

    df = df[[
        'event_date',
        'attendance',
        'current_gross_revenue',
        'predicted_gross_revenue'
    ]]

    return df

@task(log_prints = True)
def load(redshift_credentials: Dict, df: pd.DataFrame) -> None:

    FLA_Redshift(**redshift_credentials).write_to_warehouse(
        df = df,
        table_name = "forecasting_hockey_merch_rev_2425_playoffs"
    )

    return None 

########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def forecasting_hockey_merch_rev_2425_playoffs() -> None:

    # base credentials 
    redshift_credentials = get_redshift_credentials()

    # extract
    df_historical = extract_historical(redshift_credentials)
    df_historical = transform(df_historical)

    df_upcoming = extract_upcoming(redshift_credentials)
    df_upcoming = transform(df_upcoming)
    
    df = get_predictions(df_historical, df_upcoming)
    load(redshift_credentials, df)

    return None 


if __name__ == "__main__":

    forecasting_hockey_merch_rev_2425_playoffs()