import pandas as pd

from prefect import flow, task
from prefect.blocks.system import Secret

from catnip.fla_redshift import FLA_Redshift

from typing import Dict, Literal

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

def get_weighted_average(group: pd.DataFrame, field: Literal["comp", "paid"], ) -> pd.Series:

    # Calculate the weighted sum
    weighted_sum = (group[f'{field}_show_rate'] * group['weights']).sum()
    
    # Calculate the weight sum
    weight_sum = group['weights'].sum()
    
    # Calculate the weighted average
    wavg = weighted_sum / weight_sum
    
    return pd.Series({
        f'weighted_{field}_average': wavg
    })

########################################################################
### ETL ################################################################
########################################################################

@task(log_prints = True)
def extract_show_rates(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH historical AS (
            SELECT
                game_desc.season,
                game_desc.tier,
                ticket.event_date::date,
                ticket.comp_seats::float,
                ticket.paid_seats::float,
                CASE
                    WHEN ticket.is_comp = TRUE AND ticket.did_attended = TRUE THEN 1
                    ELSE 0
                END AS "comp_seats_attended",
                CASE
                    WHEN is_comp = FALSE AND did_attended = TRUE THEN 1
                    ELSE 0
                END AS "paid_seats_attended"
            FROM
                custom.cth_v_historical_ticket ticket
            INNER JOIN
                custom.cth_game_descriptions game_desc
                    ON ticket.event_datetime::date = game_desc.event_datetime::date
                    AND game_desc.season IN ('2022-23', '2023-24', '2024-25')
                    AND game_desc.event_datetime < current_date
        ),
        tier_show_rate AS (
            SELECT
                season,
                tier,
                sum(historical.comp_seats_attended)::float / nullif(sum(historical.comp_seats),0) AS "comp_show_rate",
                sum(historical.paid_seats_attended)::float / nullif(sum(historical.paid_seats),0) AS "paid_show_rate"
            FROM
                historical
            GROUP BY
                season,
                tier
        )
        SELECT
            *
        FROM
            tier_show_rate
        where
            tier in ('R1','R2','R3','SC')
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def transform_show_rates(df: pd.DataFrame) -> pd.DataFrame:

    # get weighted averages
    weights = {
        '2022-23': 0.5, 
        '2023-24': 1.,
        '2024-25': 1.50
    }

    df['weights'] = df['season'].map(weights)

    df_paid = (
        df.groupby(by=['tier'])
            .apply(lambda x: get_weighted_average(x, "paid"))
            .reset_index()
    )

    df_comp = (
        df.groupby(by=['tier'])
            .apply(lambda x: get_weighted_average(x, "comp"))
            .reset_index()
    )

    df = pd.merge(
        left = df_paid,
        right = df_comp,
        on = ['tier'],
        how = "left"
    )

    return df

@task(log_prints = True)
def extract_tickets(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        SELECT 
            event_date,
            tier,
            paid_seats
        FROM 
            custom.forecasting_hockey_tickets_2425_playoffs
        WHERE 
            event_date::date >= current_date
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def transform_and_merge(df_tickets: pd.DataFrame, df_show_rates: pd.DataFrame) -> pd.DataFrame:

    df = pd.merge(
        left = df_tickets,
        right = df_show_rates,
        on = ['tier'],
        how = "left"
    )

    print(df)

    df['predicted_turnstile'] = df['paid_seats'] * df['weighted_paid_average']

    return df

@task(log_prints = True)
def load(redshift_credentials: Dict, df: pd.DataFrame) -> None:

    # write staging
    FLA_Redshift(**redshift_credentials).write_to_warehouse(
        df = df,
        table_name = "forecasting_hockey_turnstile_2425_playoffs_staging"
    )

    # # drop where primary key
    # q = """
    #     DELETE FROM 
    #         custom.forecasting_hockey_turnstile_2425_playoffs
    #     USING 
    #         custom.forecasting_hockey_turnstile_2425_playoffs_staging 
    #     WHERE 
    #         custom.forecasting_hockey_turnstile_playoffs_2425.event_date = custom.forecasting_hockey_turnstile_2425_playoffs_staging.event_date;
    # """
    # FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    # # append rows
    # q = """
    #     ALTER TABLE custom.forecasting_hockey_turnstile_playoffs_425 APPEND FROM custom.forecasting_hockey_turnstile_2425_playoffs_staging;
    # """
    # FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    # # drop staging
    # q = """
    #     DROP TABLE custom.forecasting_hockey_turnstile_2425_playoffs_staging;
    # """
    # FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None 

@task(log_prints = True)
def create_table(redshift_credentials: Dict) -> None:

    # add additional secondary and predict revenue
    q = """
        DROP TABLE IF EXISTS custom.forecasting_hockey_tickets_2425;

        CREATE TABLE custom.forecasting_hockey_tickets_2425 AS (

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
def forecasting_hockey_turnstile_2425_playoffs() -> None:

    # redshift credentials
    redshift_credentials = get_redshift_credentials()

    df_showrates = extract_show_rates(redshift_credentials)
    df_showrates = transform_show_rates(df_showrates)

    df_tickets = extract_tickets(redshift_credentials)

    df = transform_and_merge(df_tickets=df_tickets, df_show_rates=df_showrates)

    load(redshift_credentials, df)

    create_table(redshift_credentials)

    return None 


if __name__ == "__main__":

    forecasting_hockey_turnstile_2425_playoffs()