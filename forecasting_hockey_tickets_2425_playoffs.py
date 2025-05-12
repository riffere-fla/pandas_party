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

    X_train = df[['tier_num', 'arena_level_num', 'days_out_from_event']]
    y_train = df[['cumulative_tickets']]
    
    X_test = df_future[['tier_num', 'arena_level_num', 'days_out_from_event']]

    # regualr season prediction + logitix secondary second prediction, just nightly tickets

    # is_home_team (game 1,2 or 3,4) , current_series_win, current_opponent_sereies_win, series_clincher? 

    ss = StandardScaler()
    x_train_poly = ss.fit_transform(X_train)
    x_test_poly = ss.fit_transform(X_test)

    y_log = np.nan_to_num(np.log(np.array(y_train).ravel()), nan=0.0, posinf=0.0, neginf=0.0)

    polynomial = LinearRegression().fit(x_train_poly, y_log)

    return np.exp(polynomial.predict(x_test_poly))

########################################################################
### ETL ################################################################
########################################################################

@task(log_prints = True)
def extract_historical_ticket_sales(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH base_table AS (
            SELECT
                calendar.date,
                game_desc.event_date::date,
                datediff('day', calendar.date::date, game_desc.event_date::date) AS "days_out_from_event",
                ticket.arena_level_internal
            FROM
                custom.calendar_base calendar
            CROSS JOIN
                (SELECT DISTINCT event_date FROM custom.cth_game_descriptions WHERE season IN ('2023-24', '2024-25') AND game_type = 2 AND event_date < current_date) game_desc
            CROSS JOIN
                (SELECT DISTINCT arena_level_internal FROM custom.cth_v_ticket_2324_playoffs UNION SELECT DISTINCT arena_level_internal FROM custom.cth_v_ticket_2425_playoffs) ticket
            WHERE
                calendar.year IN (2024, 2025)
                AND "days_out_from_event" BETWEEN 0 AND 50
        ),
        capacity AS (
            SELECT
                event_date,
                CASE
                    WHEN pc_one IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', '1', '2', '3', '4', '5', '6', '7', '8') THEN 'Lowers'
                    WHEN pc_one IN ('K', 'L', 'M') THEN 'Clubs'
                    WHEN pc_one IN ('N', 'O', 'P', 'Q', 'R', 'S', 'T', 'Y') THEN 'Uppers'
                    WHEN pc_one IN ('U', 'V', 'W') THEN 'Suites'
                    WHEN pc_one IN ('X', 'Z') THEN 'Premium'
                    ELSE 'Unknown'
                END AS arena_level_internal,
                CASE
                    WHEN allocations LIKE '%Kill%' OR locks LIKE '%Kill%' OR row ILIKE 'SR%' THEN 0
                    ELSE 1
                END AS capacity
            FROM
                custom.cth_v_ticket_status_2324_playoffs
            UNION ALL
            SELECT
                event_date,
                CASE
                    WHEN pc_one IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', '1', '2', '3', '4', '5', '6', '7', '8') THEN 'Lowers'
                    WHEN pc_one IN ('K', 'L', 'M') THEN 'Clubs'
                    WHEN pc_one IN ('N', 'O', 'P', 'Q', 'R', 'S', 'T', 'Y') THEN 'Uppers'
                    WHEN pc_one IN ('U', 'V', 'W') THEN 'Suites'
                    WHEN pc_one IN ('X', 'Z') THEN 'Premium'
                    ELSE 'Unknown'
                END AS arena_level_internal,
                CASE
                    WHEN allocations LIKE '%Kill%' OR locks LIKE '%Kill%' OR (row ILIKE 'SR%' AND status = 'AVAIL') THEN 0
                    ELSE 1
                END AS capacity
            FROM
                custom.cth_v_ticket_status_2425_playoffs
            WHERE
                event_date < current_date
        ),
        capacity_agg AS (
            SELECT
                event_date,
                arena_level_internal,
                SUM(capacity) AS capacity
            FROM
                capacity
            GROUP BY
                event_date,
                arena_level_internal
        ),
        ticket_sales AS (
            SELECT
                event_datetime::date AS "event_date",
                DATEDIFF('days', DATE(transaction_datetime), DATE(event_datetime)) AS "days_out_from_event",
                arena_level_internal,
                sum(
                    CASE
                        WHEN cth_v_historical_ticket.season = '2023-24' AND paid_seats = 1 THEN 0.5
                        WHEN cth_v_historical_ticket.season = '2024-25' AND paid_seats = 1 THEN 1
                        ELSE 0
                    END
                )::float AS "paid_seats"
                -- SUM(paid_seats) AS paid_seats
            FROM
                custom.cth_v_historical_ticket
            LEFT JOIN
                custom.cth_game_descriptions USING (event_datetime)
            WHERE
                (ticket_type IN ('Singles', 'Nightly Suites')
                OR (ticket_type = 'Secondary' AND price_type = 'G9 - Group Logitix'))
                AND game_type = 2
            GROUP BY
                event_datetime,
                DATE(transaction_datetime),
                arena_level_internal
        ),
        ticket_sales_cum AS (
            SELECT
                event_date,
                days_out_from_event,
                arena_level_internal,
                SUM(paid_seats) OVER (
                    PARTITION BY event_date, arena_level_internal
                    ORDER BY days_out_from_event
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_tickets
            FROM
                ticket_sales
        ),
        ticket_sales_agg AS (
            SELECT
                base_table.event_date,
                base_table.arena_level_internal,
                base_table.days_out_from_event,
                CASE
                    WHEN ticket_sales_cum.cumulative_tickets IS NULL THEN
                        LAST_VALUE(ticket_sales_cum.cumulative_tickets IGNORE NULLS) OVER (
                            PARTITION BY base_table.event_date, base_table.arena_level_internal
                            ORDER BY base_table.days_out_from_event
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )
                    ELSE
                        cumulative_tickets
            END AS "cumulative_tickets"
            FROM
                base_table
            LEFT JOIN
                ticket_sales_cum
                    ON base_table.event_date = ticket_sales_cum.event_date
                    AND base_table.days_out_from_event = ticket_sales_cum.days_out_from_event
                    AND base_table.arena_level_internal = ticket_sales_cum.arena_level_internal
        )
        SELECT
            game_desc.tier,
            (game_desc.original_six_plus_extra * 100)::int AS "original_six_plus_extra",
            nhl_api.is_series_clinchable_game,
            nhl_api.is_home_series,
            ticket_sales_agg.event_date,
            ticket_sales_agg.arena_level_internal,
            ticket_sales_agg.days_out_from_event,
            coalesce(ticket_sales_agg.cumulative_tickets, 0) AS "cumulative_tickets",
            capacity_agg.capacity,
            capacity_agg.capacity - coalesce(ticket_sales_agg.cumulative_tickets, 0) AS "capacity_remaining"
        FROM
            ticket_sales_agg
        LEFT JOIN
            capacity_agg
                ON ticket_sales_agg.event_date = capacity_agg.event_date
                AND ticket_sales_agg.arena_level_internal = capacity_agg.arena_level_internal
        LEFT JOIN
            custom.nhl_api_v_panthers_results nhl_api
                ON ticket_sales_agg.event_date = date(nhl_api.event_datetime)
        LEFT JOIN
            custom.cth_game_descriptions game_desc ON ticket_sales_agg.event_date = game_desc.event_date::date
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def extract_upcoming_games(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH capacity AS (
            SELECT
                event_date,
                CASE
                    WHEN pc_one IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', '1', '2', '3', '4', '5', '6', '7', '8') THEN 'Lowers'
                    WHEN pc_one IN ('K', 'L', 'M') THEN 'Clubs'
                    WHEN pc_one IN ('N', 'O', 'P', 'Q', 'R', 'S', 'T', 'Y') THEN 'Uppers'
                    WHEN pc_one IN ('U', 'V', 'W') THEN 'Suites'
                    WHEN pc_one IN ('X', 'Z') THEN 'Premium'
                    ELSE 'Unknown'
                END AS arena_level_internal,
                CASE
                    WHEN allocations LIKE '%Kill%' OR locks LIKE '%Kill%' OR (row ILIKE 'SR%' AND status = 'AVAIL') THEN 0
                    ELSE 1
                END AS capacity
            FROM
                custom.cth_v_ticket_status_2425_playoffs
            WHERE
                event_date >= current_date
        ),
        capacity_agg AS (
            SELECT
                event_date,
                arena_level_internal,
                SUM(capacity) AS capacity
            FROM
                capacity
            GROUP BY
                event_date,
                arena_level_internal
        ),
        ticket_sales AS (
            SELECT
                event_datetime::date AS "event_date",
                arena_level_internal,
                SUM(paid_seats) AS paid_seats
            FROM
                custom.cth_v_ticket_2425_playoffs
            WHERE
                event_datetime >= current_date
            GROUP BY
                event_datetime,
                arena_level_internal
        )
        SELECT
            game_desc.tier,
            (game_desc.original_six_plus_extra * 100)::int AS "original_six_plus_extra",
            ticket_sales.event_date,
            ticket_sales.arena_level_internal,
            DATEDIFF('days', current_date, DATE(event_datetime)) AS days_out_from_event,
            ticket_sales.paid_seats,
            capacity_agg.capacity,
            capacity_agg.capacity - coalesce(ticket_sales.paid_seats, 0) AS "capacity_remaining"
        FROM
            ticket_sales
        LEFT JOIN
            capacity_agg
                ON ticket_sales.event_date = capacity_agg.event_date
                AND ticket_sales.arena_level_internal = capacity_agg.arena_level_internal
        LEFT JOIN
            custom.cth_game_descriptions game_desc ON ticket_sales.event_date = game_desc.event_date::date
        WHERE
            tier IS NOT NULL
        ORDER BY
            ticket_sales.event_date,
            ticket_sales.arena_level_internal
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)


@task(log_prints = True)
def transform_ticket_sales(df: pd.DataFrame) -> pd.DataFrame:

    def create_tier_num(df: pd.DataFrame) -> pd.DataFrame:
        lookup = {
            "SC": 4,
            "R3": 3,
            "R2": 2,
            "R1": 1
        }

        df['tier_num'] = 0
        for key, value in lookup.items():
            df.loc[df['tier'] == key, 'tier_num'] = value

        return df
    
    def create_arena_level_num(df: pd.DataFrame) -> pd.DataFrame:
        lookup = {
            "Clubs": 5,
            "Lowers": 4,
            "Uppers": 3,
            "Suites": 2,
            "Premium": 1
        }
        df['arena_level_num'] = 0
        for key, value in lookup.items():
            df.loc[df['arena_level_internal'] == key, 'arena_level_num'] = value

        return df

    df = create_tier_num(df)
    df = create_arena_level_num(df)

    return df

@task(log_prints = True)
def get_seat_predictions(df_historical: pd.DataFrame, df_upcoming: pd.DataFrame) -> pd.DataFrame:

    # no suites
    MODEL_LEVELS = ['Lowers', 'Premium', 'Uppers', 'Clubs']
    df_final = pd.DataFrame()

    for arena_level in MODEL_LEVELS:

        df_temp = df_upcoming[df_upcoming['arena_level_internal'] == arena_level]

        df_temp['cumulative_tickets_predicted'] = run_model(
            df = df_historical[df_historical['arena_level_internal'] == arena_level],
            df_future = df_temp
        )

        df_final = pd.concat([df_final, df_temp], ignore_index = True)

    return df_final


@task(log_prints=True)
def transform(df: pd.DataFrame, df_upcoming: pd.DataFrame) -> pd.DataFrame:

    # append suites
    df_suites = df_upcoming[df_upcoming['arena_level_internal'] == "Suites"]
    df_suites['cumulative_tickets_predicted'] = [x for x in df_suites['capacity_remaining']]

    df = pd.concat([df, df_suites], ignore_index=True)

    # adjust for capacity and negative predictions
    df['cumulative_tickets_predicted'] = df.apply(
        lambda row: min(max(row['cumulative_tickets_predicted'], 0), row['capacity_remaining']),
        axis=1
    )

    def convert_to_int(x):
        try:
            return int(float(x))
        except ValueError:
            print(f"Returning NA for {x}")
            return pd.NA
    
    # total tickets
    df['total_tickets_predicted'] = df['paid_seats'] + df['cumulative_tickets_predicted']
    df['total_tickets_predicted'] = df['total_tickets_predicted'].apply(convert_to_int)

    return df 


@task(log_prints = True)
def load(redshift_credentials: Dict, df: pd.DataFrame) -> None:

    FLA_Redshift(**redshift_credentials).write_to_warehouse(
        df = df,
        table_name = "forecasting_hockey_tickets_2425_playoff_staging"
    )
    
    return None 


@task(log_prints = True)
def create_table(redshift_credentials: Dict) -> None:

    # add additional secondary and predict revenue
    q = """
        DROP TABLE IF EXISTS custom.forecasting_hockey_tickets_2425_playoffs;

        CREATE TABLE custom.forecasting_hockey_tickets_2425_playoffs AS (

            WITH temp AS (
                SELECT
                    '2024-25'::varchar AS "season",
                    game_desc.tier,
                    ticket.arena_level_internal,
                    sum(ticket.gross_revenue) / sum(ticket.paid_seats) AS "average_gross_paid_atp",
                    1.5::float AS "weight"
                FROM
                    custom.cth_v_ticket_2425_playoffs ticket
                INNER JOIN
                    custom.cth_game_descriptions game_desc
                        ON ticket.event_datetime = game_desc.event_datetime
                        AND game_desc.game_type = 2
                WHERE
                    ticket.ticket_type_playoffs IN ('Singles', 'Nightly Suites')
                GROUP BY
                    game_desc.tier,
                    ticket.arena_level_internal
                UNION ALL
                SELECT
                    '2023-24'::varchar AS "season",
                    game_desc.tier,
                    ticket.arena_level_internal,
                    sum(ticket.gross_revenue) / sum(ticket.paid_seats) AS "average_gross_paid_atp",
                    0.75::float AS "weight"
                FROM
                    custom.cth_v_ticket_2324_playoffs ticket
                INNER JOIN
                    custom.cth_game_descriptions game_desc
                        ON ticket.event_datetime = game_desc.event_datetime
                        AND game_desc.game_type = 2
                WHERE
                    ticket.ticket_type_playoffs IN ('Singles', 'Nightly Suites')
                GROUP BY
                    game_desc.tier,
                    ticket.arena_level_internal
            ),
            projected_atp AS (
                SELECT
                    tier,
                    arena_level_internal,
                    SUM(average_gross_paid_atp * weight) / NULLIF(SUM(weight), 0) * 0.85 AS "weighted_average_gross_paid_atp"
                FROM
                    temp
                GROUP BY
                    tier,
                    arena_level_internal
                ORDER BY
                    tier,
                    arena_level_internal
            ),
            current_revenue AS (
                SELECT
                    event_datetime::date::varchar AS "event_date",
                    arena_level_internal,
                    sum(gross_revenue) AS "current_gross_revenue"
                FROM
                    custom.cth_v_ticket_2425_playoffs
                GROUP BY
                    event_datetime,
                    arena_level_internal
            )
            SELECT
                ticket.*,
                current_revenue.current_gross_revenue,
                projected_atp.weighted_average_gross_paid_atp,
                coalesce(secondary.predicted_additional_secondary, 0) AS "additional_secondary",
                (ticket.cumulative_tickets_predicted * projected_atp.weighted_average_gross_paid_atp)
                    + coalesce(secondary.predicted_additional_secondary, 0)
                    + current_revenue.current_gross_revenue
                    AS "projected_gross_revenue"
            FROM
                custom.forecasting_hockey_tickets_2425_playoff_staging ticket
            LEFT JOIN
                current_revenue USING (event_date, arena_level_internal)
            LEFT JOIN
                projected_atp USING (tier, arena_level_internal)
            --         ON ticket.tier = projected_atp.tier
            --         AND ticket.arena_level_internal = projected_atp.arena_level_internal
            LEFT JOIN
                custom.cth_secondary_difference_projection_2425_playoffs secondary
                    ON ticket.event_date::date = secondary.event_date::date
                    AND ticket.arena_level_internal = 'Uppers'
            ORDER BY
                ticket.event_date,
                ticket.arena_level_internal
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
    df = transform(df, df_upcoming)
    print(df)

    # load staging
    load(redshift_credentials, df)

    # create final table w/ predicted revenue
    create_table(redshift_credentials)

    return None 


if __name__ == "__main__":

    forecasting_hockey_tickets_2425_playoffs()