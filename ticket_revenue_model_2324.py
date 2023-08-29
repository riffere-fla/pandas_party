import pandas as pd
import numpy as np
from datetime import datetime
from prefect.blocks.system import Secret
from catnip.fla_redshift import FLA_Redshift

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC

from sklearn.metrics import confusion_matrix, accuracy_score, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

def get_redshift_credentials():

    cred_dict = {
        "dbname": Secret.load("stellar-redshift-db-name").get(),
        "host": Secret.load("stellar-redshift-host").get(),
        "port": 5439,
        "user": Secret.load("stellar-redshift-user-name").get(),
        "password": Secret.load("stellar-redshift-password").get(),

        "aws_access_key_id": Secret.load("fla-s3-aws-access-key-id-east-1").get(),
        "aws_secret_access_key": Secret.load("fla-s3-aws-secret-access-key-east-1").get(),
        "bucket": Secret.load("fla-s3-bucket-name-east-1").get(),
        "subdirectory": "us-east-1",

        "verbose": False,

        # "input_schema": InputSchema,
        # "output_schema": InputSchema
    }

    return cred_dict

def extract():
    return FLA_Redshift(**get_redshift_credentials()).query_warehouse(sql_string="select top 50 * from custom.cth_game_descriptions")

if __name__ == "__main__":
    df = extract()

    df = pd.read_csv("C:\\Users\\riffere\\OneDrive - Florida Panthers\\Documents\\LTP\\Kids_Club.csv")
    # FLA_Redshift(**get_redshift_credentials()).write_to_warehouse(df = df, table_name= "Kids_Club_Combined_Data")

    q = """
    with seats as
        (select 
            distinct seat_id, pc_one
        from 
            custom.cth_manifest_2223 ),

    dates as
        (select 
            event_date
        from 
            custom.cth_game_descriptions
        where 
            season = '2022-23' and is_regular_season = 1),

    other as
        (select 
            event_date, section_name, row_name, seat,
            cast(section_name as varchar)+'-'+cast(row_name as varchar)+'-'+cast(seat as varchar) as seat_id,block_purchase_price, ticket_type
        from 
            custom.cth_ticket_expanded_all),

    base as
        (select 
            *
        from 
            dates
        cross 
            join seats)

    select 
        base.event_date, tier, base.seat_id, abbreviation, pc_one,
            CASE
                WHEN pc_one in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', '1', '2', '3', '4', '5', '6', '7', '8')
                    THEN 'Lowers'
                WHEN pc_one in ('K', 'L', 'M') THEN 'Clubs'
                WHEN pc_one in ('N', 'O', 'P', 'Q', 'R', 'S', 'T') THEN 'Uppers'
                WHEN pc_one in ('U', 'V', 'W') THEN 'Suites'
                WHEN pc_one in ('X') THEN 'Amerant'
                WHEN pc_one in ('Y') THEN 'Loft'
                WHEN pc_one in ('Z') THEN 'Corona'
                ELSE 'unknown'
                END AS location,
        CASE
            when block_purchase_price > 0 then block_purchase_price
            else 0
        end as block_purchase_price,
        CASE
            when ticket_type IS NOT NULL then ticket_type
            else 'Not Sold'
        end as ticket_type_final,
        CASE
            when ticket_type_final in ('Full', 'Annual Suites', 'Premier', 'Flex', 'Quarter', 'Sponsor', 'Trade') then 'Plans'
            when ticket_type_final in ('Groups') then 'Groups'
            when ticket_type_final in ('Secondary') then 'Secondary'
            when ticket_type_final in ('Not Sold') then 'Not Sold'
            else 'Nightly'
        end as ticket_type_group
    from 
        base
    left join 
        other on base.event_date = other.event_date and base.seat_id = other.seat_id
    left join 
        custom.cth_game_descriptions on base.event_date = cth_game_descriptions.event_date
    order by 
        base.event_date, base.seat_id
    """

    df = FLA_Redshift(**get_redshift_credentials()).query_warehouse(sql_string=q)

    df['dow'] = [datetime.weekday(datetime.strptime(x, "%Y-%m-%d")) for x in df['event_date']]
    df['tier_num'] = df.apply(lambda row: 5 if row['tier'] == 'A' else (4 if row['tier'] == 'B' else (3 if row['tier'] == 'C' else (2 if row['tier'] == 'D' else 1))), axis = 1)

    df_groupby = df.groupby(by = ['event_date', 'pc_one'])['seat_id'].count()

    df_sold = df[~df.ticket_type_final.isin(['Not Sold'])]

    df_sold_groupby = df_sold.groupby(by = ['event_date', 'pc_one'])['seat_id'].count()

    df_sold_non_nightly = df[~df.ticket_type_group.isin(['Not Sold'])]

    df_groupby_2 = df_sold_non_nightly.groupby(by = ['event_date', 'pc_one'])['seat_id'].count()
    df_by_pc_2 = df_groupby_2.to_frame().rename(columns = {'seat_id' : 'non_nightly_seats'})
    df_by_pc_2.reset_index(inplace = True)


    df_by_pc = df_groupby.to_frame().rename(columns = {'seat_id' : 'total_seats'})
    df_by_pc.reset_index(inplace = True)

    df_sold_by_pc = df_sold_groupby.to_frame().rename(columns = {'seat_id' : 'sold_seats'})
    df_sold_by_pc.reset_index(inplace = True)

    df_merge = df_by_pc.merge(right = df_sold_by_pc, how = 'left', on = ['event_date', 'pc_one'])

    df_merge['sell_thru'] = [x/y for x,y in zip(df_merge['sold_seats'], df_merge['total_seats'])]

    df_merge['new_sell_thru'] = df_merge.apply(lambda row: row['sell_thru'] if row['sell_thru'] == 1 else (1.1*row['sell_thru'] if row['sell_thru'] < 0.7 else (1.05*row['sell_thru'] if 1.05*row['sell_thru'] < 1 else 1)), axis = 1)

    df_merge['new_seats'] = [int((x*y)-z) for x,y,z in zip(df_merge['total_seats'], df_merge['new_sell_thru'], df_merge['sold_seats'])]

    df_sold_by_pc_nightly = df[(df['ticket_type_group'] == 'Nightly') & (df['block_purchase_price'] > 0)]

    df_merge_2 = df_sold_by_pc_nightly.groupby(by = ['tier', 'pc_one'])['block_purchase_price'].mean()
    df_merge_2 = df_merge_2.to_frame().rename(columns = {'seat_id' : 'total_seats'})
    df_merge_2.reset_index(inplace = True)

    q = """
    select distinct event_date, tier
    from custom.cth_game_descriptions
    where season = '2022-23'
    and tier not like '%P%'
    and tier not like '%S%'
    """

    df2 = FLA_Redshift(**get_redshift_credentials()).query_warehouse(sql_string=q)

    df_merge = df_merge.merge(right = df2, how = 'left', on = ['event_date'])

    df_merge = df_merge.merge(right = df_merge_2, how = 'left', on = ['tier', 'pc_one'])

    df_merge = df_merge.merge(right = df_by_pc_2, how = 'left', on = ['event_date', 'pc_one'])

    df_merge['leftover_seats'] = [x-y for x,y in zip(df_merge['total_seats'], df_merge ['non_nightly_seats'])]

    df_merge['add_rev'] = [x*y for x,y in zip(df_merge['leftover_seats'], df_merge['block_purchase_price'])]

    df_merge_2 = df_sold.groupby(by = ['event_date'])['block_purchase_price'].sum()

    # print(df_merge.groupby(by = ['event_date'])['add_rev'].sum())

    pcs = sorted(df['pc_one'].unique())
    pc_dict = dict((value,count) for count, value in enumerate(pcs))
    df['pc_num'] = df.apply(lambda row: pc_dict[row['pc_one']], axis = 1)

    X = df[['dow', 'tier_num', 'pc_num']]
    y = df[['ticket_type_group']]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=1993)
    ss = StandardScaler()
    x_train_scaled = ss.fit_transform(X_train)
    x_test_scaled = ss.fit_transform(X_test)

    clf = RandomForestClassifier() # 7.7% different, 55.6% accurate
    clf.fit(x_train_scaled, y_train)

    knn = KNeighborsClassifier(weights = 'distance') # 11.6% different, 49.4% accurate
    knn.fit(x_train_scaled, y_train)

    predicted = clf.predict(x_train_scaled)
    cm = confusion_matrix(y_train.values,predicted, labels = ['Nightly', 'Secondary', 'Not Sold', 'Plans', 'Groups'])
    print('RFC')
    print(accuracy_score(y_train.values,predicted))
    print(pd.DataFrame(cm))

    predicted = knn.predict(x_train_scaled)
    cm = confusion_matrix(y_train.values,predicted, labels = ['Nightly', 'Secondary', 'Not Sold', 'Plans', 'Groups'])
    print('KNN')
    print(accuracy_score(y_train.values,predicted))
    print(pd.DataFrame(cm))

    # disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=['Nightly', 'Secondary', 'Not Sold', 'Plans', 'Groups'])
    #print(df_merge_2)