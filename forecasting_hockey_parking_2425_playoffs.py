import pandas as pd

from prefect import flow, task
from prefect.blocks.system import Secret

from catnip.fla_redshift import FLA_Redshift

from typing import Dict, Literal

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

def run_model(df: pd.DataFrame, df_future: pd.DataFrame, parking_lot: str, parking_type: Literal['prepaid', 'onsite']) -> np.ndarray:

    if parking_type == "onsite":
        x_train = df[['capacity_remaining','is_weekend','start_time_num']]
    elif parking_type == "prepaid":
        x_train = df[['days_out', 'is_weekend', 'start_time_num']]

    if parking_type == "onsite":
        y_train = df[['num_cars']]
    elif parking_type == "prepaid":
        y_train = df[['num_passes_cum']]

    if parking_type == "onsite":
        x_test = df_future[['capacity_remaining','is_weekend','start_time_num']]
    elif parking_type == "prepaid":
        x_test = df_future[['days_out','is_weekend','start_time_num']]

    scalar = StandardScaler()
    poly_features = scalar.fit_transform(x_train)

    polynomial = LinearRegression().fit(poly_features, np.array(y_train).ravel())

    poly_features2 = scalar.fit_transform(x_test)

    return polynomial.predict(poly_features2)

########################################################################
### ETL ################################################################
########################################################################

@task(log_prints = True)
def extract_prepaid_show_rates(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH prepaid AS (
            SELECT
                event_datetime,
                location_group
            FROM
                custom.ctp_v_ticket_2324
            WHERE
                event_type ILIKE '%panthers%'
                AND event_datetime < CURRENT_DATE
            UNION ALL
            SELECT
                event_datetime,
                location_group
            FROM
                custom.ctp_v_ticket_2425
            WHERE
                event_type ILIKE '%panthers%'
                AND event_datetime < CURRENT_DATE
        ),
        prepaid_agg AS (
            SELECT
                event_datetime,
                location_group,
                COUNT(*) AS prepaid_passes
            FROM
                prepaid
            GROUP BY
                event_datetime, 
                location_group
        ),
        scans AS (
            SELECT
                season,
                cth_game_descriptions.event_datetime,
                tier,
                location_group,
                CASE
                    WHEN paid_amount = 0 THEN 1
                    ELSE 0 
                END AS num_scans
            FROM
                custom.parkhub_v_transactions
            LEFT JOIN
                custom.cth_game_descriptions 
                ON parkhub_v_transactions.event_datetime = cth_game_descriptions.event_datetime
            WHERE
                cth_game_descriptions.event_datetime IS NOT NULL
                AND season IN ('2023-24', '2024-25')
        ),
        scans_agg AS (
            SELECT
                season,
                event_datetime,
                tier,
                location_group,
                SUM(num_scans) AS num_scans
            FROM
                scans
            GROUP BY
                season,
                event_datetime,
                tier,
                location_group
        )
        SELECT
            season,
            prepaid_agg.event_datetime,
            tier,
            prepaid_agg.location_group,
            prepaid_passes,
            num_scans,
            num_scans * 1.0 / prepaid_passes::FLOAT AS prepaid_show_rate
        FROM
            scans_agg
        LEFT JOIN
            prepaid_agg 
            ON scans_agg.event_datetime = prepaid_agg.event_datetime
            AND scans_agg.location_group = prepaid_agg.location_group
        WHERE
            prepaid_agg.event_datetime IS NOT NULL
            AND tier IN ('R1','R2','R3','SC')
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)


def transform_prepaid_show_rates(df: pd.DataFrame) -> pd.DataFrame:

    # weigh this season more heavily
    weights = {
        '2023-24': 1.25,
        '2024-25': 2.75
    }
    df['weights'] = df['season'].map(weights)
    
    # Calculate the weighted average
    def weighted_paid_average(group):
        # Calculate the weighted sum
        weighted_sum = (group['prepaid_show_rate'] * group['weights']).sum()
        
        # Calculate the weight sum
        weight_sum = group['weights'].sum()
        
        # Calculate the weighted average
        wavg = weighted_sum / weight_sum
        
        return pd.Series({
            'prepaid_show_rate_weighted_average': wavg
        })

    df = df.groupby(by = ['tier','location_group']).apply(weighted_paid_average).reset_index()  

    return df 


@task(log_prints = True)
def extract_prepaid_transactions(redshift_credentials: Dict) -> pd.DataFrame:

    q = """            
        WITH crossed AS (
            SELECT
                calendar.date,
                game_desc.event_date,
                datediff('day', calendar.date::date, game_desc.event_date::date) AS "days_out",
                ticket.location_group
            FROM
                custom.calendar_base calendar
            CROSS JOIN
                (SELECT DISTINCT event_date FROM custom.cth_game_descriptions WHERE season IN ('2023-24', '2024-25') AND game_type = 1) game_desc
            CROSS JOIN
                (SELECT DISTINCT location_group FROM custom.ctp_v_ticket_2425) ticket
            WHERE
                calendar.year IN (2023, 2024, 2025)
                AND "days_out" BETWEEN 0 AND 200
        ),
        prepaid AS (
            SELECT
                event_datetime,
                location_group,
                DATE(transaction_date) AS transaction_date,
                datediff('day', transaction_date::date, event_datetime::date) AS "days_out"
            FROM
                custom.ctp_v_ticket_2324
            WHERE
                event_type ILIKE '%panthers%'
                AND event_datetime < CURRENT_DATE
            UNION ALL
            SELECT
                event_datetime,
                location_group,
                DATE(transaction_date) AS transaction_date,
                datediff('day', transaction_date::date, event_datetime::date) AS "days_out"
            FROM
                custom.ctp_v_ticket_2425
            WHERE
                event_type ILIKE '%panthers%'
                AND event_datetime < CURRENT_DATE
        )
        SELECT
            DATE(crossed.event_date) AS event_date,
            crossed.location_group,
            'prepaid' AS parking_type,
            DATEDIFF('days', transaction_date::date, crossed.event_date::date) AS days_out,
            CASE
                WHEN DATEDIFF('days', transaction_date::date, crossed.event_date::date) >= 150 THEN 0
                ELSE COUNT(*)::int
            END AS num_passes
        FROM
            crossed
        LEFT JOIN
            prepaid
                ON crossed.event_date::date = prepaid.event_datetime::date
                AND crossed.days_out = prepaid.days_out
                AND crossed.location_group = prepaid.location_group
        LEFT JOIN
            custom.cth_game_descriptions
            ON prepaid.event_datetime = cth_game_descriptions.event_datetime
        WHERE
            DATEDIFF('days', transaction_date, prepaid.event_datetime) >= 0
            AND tier IN ('R1', 'R2', 'R3', 'SC')
        GROUP BY
            crossed.event_date,
            crossed.location_group,
            transaction_date
        ORDER BY
            crossed.event_date,
            crossed.location_group,
            transaction_date DESC
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def transform_prepaid_transactions(df: pd.DataFrame) -> pd.DataFrame:

    def create_location_group_num(df: pd.DataFrame) -> pd.DataFrame:
        # Define the dictionary with lists of start times
        lookup = {
            1: "Valet",
            2: "General",
            3: "Garage",
            4: "Club"
        }

        # Default to 0 for any start time not in the lookup
        df['location_group_num'] = 0
        for key, value in lookup.items():
            df.loc[df['location_group'] == value, 'location_group_num'] = key

        return df


    df = create_location_group_num(df)

    df = df.sort_values(by=['event_date', 'location_group', 'days_out'])
    df['num_passes_cum'] = df.groupby(['event_date', 'location_group'])['num_passes'].cumsum()
    df['num_passes_cum'] = df.groupby(['event_date', 'location_group'])['num_passes_cum'].ffill()

    return df

@task(log_prints = True)
def extract_all_tickets(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        SELECT
            summary.event_date::date,
            game_desc.tier,
            game_desc.day_of_week,
            game_desc.start_time,
            calendar.is_weekend,
            summary.total_tickets,
            summary.total_seats_attended
        FROM
            custom.cth_v_historical_attendance_summary summary
        INNER JOIN
            custom.cth_game_descriptions game_desc
                ON summary.event_date::date = game_desc.event_date::date
                AND game_desc.game_type = 1
        LEFT JOIN
            custom.calendar_base calendar ON summary.event_date::date = calendar.date::date
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints = True)
def transform_all_tickets(df: pd.DataFrame) -> pd.DataFrame:

    def create_start_time_group(df: pd.DataFrame) -> pd.DataFrame:
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

    df = create_start_time_group(df)

    return df

@task(log_prints = True)
def extract_current_parking(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        SELECT
            ticket.event_datetime::date AS "event_date",
            datediff('day', current_date, ticket.event_datetime) AS "days_out",
            ticket.location_group,
            capacities.capacity::int,
            sum(ticket.paid_seats + ticket.comp_seats) AS "current_prepaid_passes",
            greatest(capacities.capacity::int - "current_prepaid_passes", 0) AS "current_cap_remaining"
        FROM
            custom.ctp_v_ticket_2425 ticket
        LEFT JOIN
            custom.ctp_parking_capacities capacities ON ticket.location_group = capacities.location_group
        INNER JOIN
            custom.cth_game_descriptions game_desc
                ON ticket.event_datetime = game_desc.event_datetime
                AND game_desc.event_datetime > CURRENT_DATE
        GROUP BY
            ticket.event_datetime,
            ticket.location_group,
            capacities.capacity
        ORDER BY
            ticket.event_datetime,
            ticket.location_group
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)


@task(log_prints=True)
def merge(df_tickets: pd.DataFrame, df_parking: pd.DataFrame) -> pd.DataFrame:

    df = pd.merge(
        left = df_parking,
        right = df_tickets,
        how = "inner",
        on = "event_date"
    )

    return df 

@task(log_prints=True)
def get_prepaid_predictions(df_prepaid: pd.DataFrame, df_current: pd.DataFrame, df_show_rates: pd.DataFrame) -> pd.DataFrame:

    LOTS = ['General', 'Garage']
    df = pd.DataFrame()

    for parking_lot in LOTS:

        temp = df_current[df_current['location_group'] == parking_lot]

        temp['expected_additional_prepaid_passes'] = run_model(
            df=df_prepaid[df_prepaid['location_group'] == parking_lot],
            df_future=temp,
            parking_lot=parking_lot,
            parking_type="prepaid"
        )

        df = pd.concat([temp, df], ignore_index=True)

    # concat club totals
    club_totals = df_current[df_current['location_group'] == 'Club']
    club_totals['expected_additional_prepaid_passes'] = 0

    df = pd.concat([df, club_totals], axis=0)

    # make sure no negative predictions are made
    df['expected_additional_prepaid_passes'] = [x if x > 0 else x for x in df['expected_additional_prepaid_passes']]

    # get total prepaid tickets (current + predicted additional)
    df['total_expected_prepaid_passes'] = df['current_prepaid_passes'] + df['expected_additional_prepaid_passes']

    # get number of parked cars using historical show rates
    df = pd.merge(
        left = df,
        right = df_show_rates,
        how = "left",
        on = ['tier', 'location_group']
    )

    df['expected_prepaid_passes_parked'] = df['total_expected_prepaid_passes'] * df['prepaid_show_rate_weighted_average']

    # find the capacity remaining 
    df['capacity_remaining'] = df['capacity'] - df['expected_prepaid_passes_parked']

    # if predicted cars over capacity subtract overflow out
    df['expected_additional_prepaid_passes'] = np.where(
        df['capacity_remaining'] < 0, 
        df['expected_additional_prepaid_passes'] + df['capacity_remaining'], 
        df['expected_additional_prepaid_passes']
    )
    df['expected_prepaid_passes_parked'] = np.where(
        df['capacity_remaining'] < 0, 
        df['expected_prepaid_passes_parked'] + df['capacity_remaining'], 
        df['expected_prepaid_passes_parked']
    )
    df['capacity_remaining'] = np.where(df['capacity_remaining'] < 0, 0, df['expected_prepaid_passes_parked'])

    # filter columns
    df = df[[
        'event_date',
        'days_out',
        'tier',
        'start_time_num',
        'is_weekend',
        'location_group',
        'capacity',
        'current_prepaid_passes',
        'expected_additional_prepaid_passes',
        'total_expected_prepaid_passes',
        'expected_prepaid_passes_parked',
        'capacity_remaining'
    ]]

    return df 


@task(log_prints = True)
def extract_historical_onsite(redshift_credentials: Dict) -> pd.DataFrame:

    q = """
        WITH onsite AS (
            SELECT
                parkhub.event_datetime::date AS "event_date",
                parkhub.location_group,
                0 AS "days_out",
                sum(CASE WHEN parkhub.paid_amount > 0 THEN 1 ELSE 0 END) AS "num_onsite_cars",
                sum(CASE WHEN parkhub.paid_amount = 0 THEN 1 ELSE 0 END) AS "num_prepaid_cars"
            FROM
                custom.parkhub_v_transactions parkhub
            INNER JOIN
                custom.cth_game_descriptions game_desc
                    ON parkhub.event_datetime::date = game_desc.event_datetime::date
                    AND game_desc.season IN ('2023-24', '2024-25')
            GROUP BY
                parkhub.event_datetime::date,
                parkhub.location_group
        )
        SELECT
            'onsite' AS parking_type,
            onsite.event_date,
            onsite.location_group,
            onsite.days_out,
            onsite.num_onsite_cars AS "num_cars",
            capacities.capacity - onsite.num_prepaid_cars AS "capacity_remaining"
        FROM
            onsite
        LEFT JOIN
            custom.ctp_parking_capacities capacities ON onsite.location_group = capacities.location_group
    """

    return FLA_Redshift(**redshift_credentials).query_warehouse(sql_string=q)

@task(log_prints=True)
def get_onsite_predictions(df_onsite: pd.DataFrame, df_current: pd.DataFrame) -> pd.DataFrame:

    LOTS = ['General','Garage','Club']
    df = pd.DataFrame()

    for parking_lot in LOTS:

        temp = df_current[df_current['location_group'] == parking_lot]

        temp['predicted_onsite_parking'] = run_model(
            df=df_onsite[df_onsite['location_group'] == parking_lot],
            df_future=temp,
            parking_lot=parking_lot,
            parking_type="onsite"
        )

        df = pd.concat([temp, df], ignore_index=True)

    # if predicted total over capacity subtract overflow out
    df['expected_onsite_parking'] = [pred_onsite if pred_onsite <= cap_remaining else cap_remaining for pred_onsite, cap_remaining in zip(df['predicted_onsite_parking'], df['capacity_remaining'])]
    df['total_expected_cars_parked'] = df['expected_prepaid_passes_parked'] + df['predicted_onsite_parking']

    df = df[[
        'event_date', 
        'days_out',
        'tier',
        'location_group',
        'capacity',
        'current_prepaid_passes',
        'expected_additional_prepaid_passes',
        'total_expected_prepaid_passes',
        'expected_prepaid_passes_parked',
        'expected_onsite_parking',
        'total_expected_cars_parked'
    ]]

    return df 

@task(log_prints=True)
def get_additional_lots(df_current: pd.DataFrame, df_show_rates: pd.DataFrame) -> pd.DataFrame:

    df = df_current[df_current['location_group'].isin(['Executive','Valet'])]

    df['expected_additional_prepaid_passes'] = 0
    df['total_expected_prepaid_passes'] = df['current_prepaid_passes']

    df = pd.merge(
        left = df,
        right = df_show_rates, 
        how =  "left", 
        on = ['tier', 'location_group']
    )

    df['prepaid_show_rate_weighted_average'] = df['prepaid_show_rate_weighted_average'].fillna(1)

    df['expected_prepaid_passes_parked'] = df['total_expected_prepaid_passes'] * df['prepaid_show_rate_weighted_average']
    df['expected_onsite_parking'] = 0
    df['total_expected_cars_parked'] = df['expected_prepaid_passes_parked'] 

    df = df[[
        'event_date', 
        'days_out',
        'tier',
        'location_group',
        'capacity',
        'current_prepaid_passes',
        'expected_additional_prepaid_passes',
        'total_expected_prepaid_passes',
        'expected_prepaid_passes_parked',
        'expected_onsite_parking',
        'total_expected_cars_parked'
    ]]

    return df 


@task(log_prints = True)
def load(redshift_credentials: Dict, df: pd.DataFrame) -> None:

    FLA_Redshift(**redshift_credentials).write_to_warehouse(
        df = df,
        table_name = "forecasting_hockey_parking_2425_staging"
    )
    
    return None 

@task(log_prints = True)
def create_table(redshift_credentials: Dict) -> None:

    q = """
        DROP TABLE IF EXISTS custom.forecasting_hockey_parking_2425_playoffs;

        CREATE TABLE custom.forecasting_hockey_parking_2425_playoffs AS (

            WITH parkhub AS (
                SELECT
                    parkhub.event_datetime,
                    parkhub.location_group,
                    sum(CASE WHEN parkhub.paid_amount = 0 THEN 1 ELSE 0 END) AS "prepaid_passes_scanned",
                    sum(CASE WHEN parkhub.paid_amount <> 0 THEN 1 ELSE 0 END) AS "onsite_passes_scanned",
                    "prepaid_passes_scanned" + "onsite_passes_scanned" AS "total_cars_parked",
                    sum(paid_amount) AS "onsite_gross_revenue",
                    sum(paid_amount) / 1.07 AS "onsite_net_revenue"
                FROM
                    custom.parkhub_v_transactions parkhub
                INNER JOIN
                    custom.cth_game_descriptions game_desc
                        ON parkhub.event_datetime = game_desc.event_datetime
                        AND game_desc.season IN ('2023-24','2024-25')
                GROUP BY
                    parkhub.event_datetime,
                    parkhub.location_group
            ),
            temp_agg AS (
                SELECT
                    event_datetime,
                    ticket.location_group,
                    count(*) AS "current_prepaid_passes",
                    0 AS "expected_additional_prepaid_passes",
                    count(*) AS "total_expected_prepaid_passes",
                    sum(ticket.gross_revenue) AS "current_gross_revenue",
                    sum(ticket.net_revenue) AS "current_net_revenue"
                FROM
                    custom.ctp_v_ticket_2425 ticket
                GROUP BY
                    event_datetime,
                    ticket.location_group
            ),
            temp AS (
                SELECT
                    event_datetime::date::varchar AS "event_date",
                    datediff('day', current_date, event_datetime) AS "days_out",
                    game_desc.tier,
                    temp_agg.location_group,
                    capacities.capacity::int,
                    temp_agg.current_prepaid_passes,
                    temp_agg.expected_additional_prepaid_passes,
                    temp_agg.total_expected_prepaid_passes,
                    CASE
                        WHEN temp_agg.location_group = 'Executive' THEN temp_agg.total_expected_prepaid_passes
                        ELSE parkhub.prepaid_passes_scanned
                    END AS "expected_prepaid_passes_parked",
                    coalesce(parkhub.onsite_passes_scanned, 0) AS "expected_onsite_parking",
                    CASE
                        WHEN temp_agg.location_group = 'Executive' THEN temp_agg.total_expected_prepaid_passes
                        ELSE parkhub.total_cars_parked
                    END AS "total_expected_cars_parked",
                    temp_agg.current_gross_revenue + coalesce(parkhub.onsite_gross_revenue, 0) AS "current_gross_revenue",
                    temp_agg.current_net_revenue + coalesce(parkhub.onsite_net_revenue, 0) AS "current_net_revenue"
                FROM
                    temp_agg
                INNER JOIN
                    custom.cth_game_descriptions game_desc USING (event_datetime)
                LEFT JOIN
                    custom.ctp_parking_capacities capacities USING (location_group)
                LEFT JOIN
                    parkhub USING (event_datetime, location_group)
            ),
            pricing AS (
                SELECT
                    event_datetime::date::varchar AS "event_date",
                    ticket.location_group,
                    max(ticket.transaction_date) AS "transaction_date",
                    max(ticket.adjusted_price) AS "prepaid_price",
                    max(ticket.gross_revenue) AS "prepaid_gross",
                    "prepaid_price" / .75 AS "gate_price",
                    "gate_price" * 1.07 AS "gate_price_gross"
                FROM
                    custom.ctp_v_ticket_2425 ticket
                INNER JOIN
                    custom.cth_game_descriptions game_desc USING (event_datetime)
                WHERE
                    ticket.is_comp = FALSE
                    AND ticket.price_type ILIKE 'IA%'
                GROUP BY
                    event_datetime, ticket.location_group
                ORDER BY
                    event_datetime, ticket.location_group
            )
            -- get all finished games
            SELECT
                event_date,
                greatest(days_out, 0) AS "days_outt",
                tier,
                location_group,
                capacity,
                current_prepaid_passes,
                expected_additional_prepaid_passes,
                total_expected_prepaid_passes,
                expected_prepaid_passes_parked,
                expected_onsite_parking,
                total_expected_cars_parked,
                current_gross_revenue AS "total_expected_gross_revenue",
                current_net_revenue AS "total_expected_net_revenue"
            FROM
                temp
            WHERE
                event_date::date < CURRENT_DATE
            UNION ALL

            -- union all upcoming games from forecast and append pricing
            SELECT
                forecast.event_date,
                forecast.days_out,
                forecast.tier,
                forecast.location_group,
                forecast.capacity,
                forecast.current_prepaid_passes,
                forecast.expected_additional_prepaid_passes,
                forecast.total_expected_prepaid_passes,
                forecast.expected_prepaid_passes_parked,
                forecast.expected_onsite_parking,
                forecast.total_expected_cars_parked,
                CASE
                    WHEN pricing.prepaid_gross IS NULL THEN
                        coalesce(temp.current_gross_revenue, 0)
                            + ((coalesce(temp.current_gross_revenue, 0)/forecast.current_prepaid_passes) * forecast.expected_additional_prepaid_passes)
                            + (((coalesce(temp.current_gross_revenue, 0)/forecast.current_prepaid_passes) / .75) * forecast.expected_onsite_parking)
                    ELSE
                        coalesce(temp.current_gross_revenue, 0)
                            + (coalesce(pricing.prepaid_gross, 1) * forecast.expected_additional_prepaid_passes)
                            + (coalesce(pricing.gate_price_gross, 1) * forecast.expected_onsite_parking)
                END AS "total_expected_gross_revenue",
                CASE
                    WHEN pricing.prepaid_price IS NULL THEN
                        coalesce(temp.current_net_revenue, 0)
                            + ((coalesce(temp.current_net_revenue, 0)/forecast.current_prepaid_passes) * forecast.expected_additional_prepaid_passes)
                            + (((coalesce(temp.current_net_revenue, 0)/forecast.current_prepaid_passes) / .75) * forecast.expected_onsite_parking)
                    ELSE
                        coalesce(temp.current_net_revenue, 0)
                            + (coalesce(pricing.prepaid_price, 1) * forecast.expected_additional_prepaid_passes)
                            + (coalesce(pricing.gate_price, 1) * forecast.expected_onsite_parking)
                END AS "total_expected_net_revenue"
            FROM
                custom.forecasting_hockey_parking_2425_staging forecast
            LEFT JOIN
                temp USING (event_date, location_group)
            LEFT JOIN
                pricing USING (event_date, location_group)
            ORDER BY
                event_date,
                location_group
        );
    """
    FLA_Redshift(**redshift_credentials).execute_and_commit(sql_string=q)

    return None 

########################################################################
### FLOW ###############################################################
########################################################################

@flow(log_prints=True)
def forecasting_hockey_parking_2425_playoffs() -> None:

    # base credentials
    redshift_credentials = get_redshift_credentials()


    # prepaid | extract and transform
    df_prepaid_show_rates = extract_prepaid_show_rates(redshift_credentials)
    df_prepaid_show_rates = transform_prepaid_show_rates(df_prepaid_show_rates)
    print(df_prepaid_show_rates)
    
    df_prepaid_transactions = extract_prepaid_transactions(redshift_credentials)
    df_prepaid_transactions = transform_prepaid_transactions(df_prepaid_transactions)
    print(df_prepaid_transactions)

    df_all_tickets = extract_all_tickets(redshift_credentials)
    df_all_tickets = transform_all_tickets(df_all_tickets)
    print(df_all_tickets)

    df_current_parking = extract_current_parking(redshift_credentials)
    print(df_current_parking)

    # prepaid | merge
    df_prepaid = merge(df_all_tickets, df_prepaid_transactions)
    print(df_prepaid)
    df_current = merge(df_all_tickets, df_current_parking)
    print(df_current)

    # prepaid | run model
    df_prepaid_prediction = get_prepaid_predictions(df_prepaid, df_current, df_prepaid_show_rates)
    print(df_prepaid_prediction)


    # onsite | extract and transform
    df_historical_onsite = extract_historical_onsite(redshift_credentials)
    df_historical_onsite = merge(df_all_tickets, df_historical_onsite)
    print(df_historical_onsite)

    # onsite | run model
    df_onsite_prediction = get_onsite_predictions(df_historical_onsite, df_prepaid_prediction)
    print(df_onsite_prediction)


    # create additional losts (exce and valet)
    df_additional = get_additional_lots(df_current, df_prepaid_show_rates)
    print(df_additional)


    # final merge
    df = pd.concat([df_onsite_prediction, df_additional], ignore_index=True)


    # load
    load(redshift_credentials, df)
    create_table(redshift_credentials)

    return None 


if __name__ == "__main__":

    forecasting_hockey_parking_2425_playoffs()