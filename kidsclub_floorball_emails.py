#from catnip import Fla_Sharepoint
import pandas as pd
import numpy as np
from catnip.fla_redshift import FLA_Redshift
#from sqlalchemy import null
from datetime import datetime
from prefect.blocks.system import Secret

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
    FLA_Redshift(**get_redshift_credentials()).write_to_warehouse(df = df, table_name= "Kids_Club_Combined_Data")
    print(df)
    # df = pd.read_csv("C:\\Users\\riffere\\OneDrive - Florida Panthers\\Documents\\LTP\\Kids_Club.csv")
    # FLA_Redshift(**get_redshift_credentials()).write_to_warehouse(df = df, table_name= "Kids_Club_Combined_Data")
    # print(df)

# q = """
# WITH ALL_KC_EMAILS AS
#     (SELECT season, lower(parent_email) as "parent_email", transaction_date, kc_group
#      FROM custom.kids_club_combined_data
#      WHERE season = '2022-23'),

# STMS_1718 AS
#     (SELECT
#          lower(emailaddr) AS "emailaddr", blockpurchaseprice,
#          CASE
#              WHEN "type" IN ('Annual Suites', 'Full', 'Half', 'Premier') THEN 1
#              ELSE 0
#          END AS "is_stm"
#      FROM CUSTOM.tickethockey1718 v),

# STMS_1718_AGG AS
#     (SELECT emailaddr, sum(blockpurchaseprice) AS "rev", max(is_stm) AS "is_stm"
#     FROM STMS_1718 a
#     GROUP BY emailaddr
#     ORDER BY emailaddr),

# SEASON_1718 AS
#     (SELECT a.parent_email, s.rev, is_stm, '2017-18' AS "season"
#     FROM STMS_1718_AGG s
#     INNER JOIN ALL_KC_EMAILS a ON s.emailaddr = a.parent_email
#     --WHERE is_stm <> 0
#     --GROUP BY email, s.is_stm
#     ),

# STMS_1819 AS
#     (SELECT
#          lower(emailaddr) AS "emailaddr", blockpurchaseprice,
#          CASE
#              WHEN "type" IN ('Annual Suites', 'Full', 'Half', 'Premier') THEN 1
#              ELSE 0
#          END AS "is_stm"
#      FROM CUSTOM.tickethockey1819 v),

# STMS_1819_AGG AS
#     (SELECT emailaddr, sum(blockpurchaseprice) AS "rev", max(is_stm) AS "is_stm"
#     FROM STMS_1819 a
#     GROUP BY emailaddr
#     ORDER BY emailaddr),

# SEASON_1819 AS
#     (SELECT a.parent_email, s.rev, is_stm, '2018-19' AS "season"
#     FROM STMS_1819_AGG s
#     INNER JOIN ALL_KC_EMAILS a ON s.emailaddr = a.parent_email
#     --WHERE is_stm <> 0
#     --GROUP BY email, s.is_stm
#     ),

# STMS_1920 AS
#     (SELECT
#          lower(emailaddr) AS "emailaddr", blockpurchaseprice,
#          CASE
#              WHEN "type" IN ('Annual Suites', 'Full', 'Half', 'Premier') THEN 1
#              ELSE 0
#          END AS "is_stm"
#      FROM CUSTOM.tickethockey1920 v),

# STMS_1920_AGG AS
#     (SELECT emailaddr, sum(blockpurchaseprice) AS "rev", max(is_stm) AS "is_stm"
#     FROM STMS_1920 a
#     GROUP BY emailaddr
#     ORDER BY emailaddr),

# SEASON_1920 AS
#     (SELECT a.parent_email, s.rev, is_stm, '2019-20' AS "season"
#     FROM STMS_1920_AGG s
#     INNER JOIN ALL_KC_EMAILS a ON s.emailaddr = a.parent_email
#     --WHERE is_stm <> 0
#     --GROUP BY email, s.is_stm
#     ),

# STMS_2021 AS
#     (SELECT
#          lower(emailaddr) AS "emailaddr", blockpurchaseprice,
#          CASE
#              WHEN "type" IN ('Annual Suites', 'Full', 'Half', 'Premier') THEN 1
#              ELSE 0
#          END AS "is_stm"
#      FROM CUSTOM.vhockey2021 v),

# STMS_2021_AGG AS
#     (SELECT emailaddr, sum(blockpurchaseprice) AS "rev", max(is_stm) AS "is_stm"
#     FROM STMS_2021 a
#     GROUP BY emailaddr
#     ORDER BY emailaddr),

# SEASON_2021 AS
#     (SELECT a.parent_email, s.rev, is_stm, '2020-21' AS "season"
#     FROM STMS_2021_AGG s
#     INNER JOIN ALL_KC_EMAILS a ON s.emailaddr = a.parent_email
#     --WHERE is_stm <> 0
#     --GROUP BY email, s.is_stm
#     ),

# STMS_2122 AS
#     (SELECT
#          lower(emailaddr) AS "emailaddr", blockpurchaseprice,
#          CASE
#              WHEN "type" IN ('Annual Suites', 'Full', 'Half', 'Premier') THEN 1
#              ELSE 0
#          END AS "is_stm"
#      FROM CUSTOM.vhockey2122 v),

# STMS_2122_AGG AS
#     (SELECT emailaddr, sum(blockpurchaseprice) AS "rev", max(is_stm) AS "is_stm"
#     FROM STMS_2122 a
#     GROUP BY emailaddr
#     ORDER BY emailaddr),

# SEASON_2122 AS
#     (SELECT a.parent_email, s.rev, is_stm, '2021-22' AS "season"
#     FROM STMS_2122_AGG s
#     INNER JOIN ALL_KC_EMAILS a ON s.emailaddr = a.parent_email
#     --WHERE is_stm <> 0
#     --GROUP BY email, s.is_stm
#     ),

# STMS_2223 AS
#     (SELECT
#          lower(a.email_addr) AS "emailaddr", block_purchase_price,
#          CASE
#                  WHEN "ticket_type" IN ('Annual Suites', 'Full', 'Half', 'Premier') THEN 1
#                  ELSE 0
#              END AS "is_stm"
#      FROM CUSTOM.cth_ticket_expanded_all v
#      LEFT JOIN custom.arch_raw_v_customer a ON v.acct_id = a.acct_id),

# STMS_2223_AGG AS
#     (SELECT emailaddr, sum(block_purchase_price) AS "rev", max(is_stm) AS "is_stm"
#     FROM STMS_2223 a
#     GROUP BY emailaddr
#     ORDER BY emailaddr),

# SEASON_2223 AS
#     (SELECT a.parent_email, s.rev, s.is_stm, '2022-23' AS "season"
#     FROM STMS_2223_AGG s
#     INNER JOIN ALL_KC_EMAILS a ON s.emailaddr = a.parent_email
#     --WHERE is_stm <> 0
#     --GROUP BY email, s.is_stm
#     )

# SELECT ALL_KC_EMAILS.parent_email, ALL_KC_EMAILS.season, kc_group, CAST(add_date AS date) AS "add_date", ALL_KC_EMAILS.transaction_date,
#        coalesce(SEASON_1718.rev, 0) AS rev1718, coalesce(SEASON_1819.rev, 0) AS rev1819,
#        coalesce(SEASON_1920.rev, 0) AS rev1920, coalesce(SEASON_2021.rev, 0) AS rev2021,
#        coalesce(SEASON_2122.rev, 0) AS rev2122, coalesce(SEASON_2223.rev, 0) AS rev2223,
#        coalesce(SEASON_1718.is_stm, 0) AS "is_stm_1718", coalesce(SEASON_1819.is_stm, 0) AS "is_stm_1819",
#        coalesce(SEASON_1920.is_stm, 0) AS "is_stm_1920", coalesce(SEASON_2021.is_stm, 0) AS "is_stm_2021",
#        coalesce(SEASON_2122.is_stm, 0) AS "is_stm_2122", coalesce(SEASON_2223.is_stm, 0) AS "is_stm_2223"
# FROM ALL_KC_EMAILS
# LEFT JOIN SEASON_1718 ON ALL_KC_EMAILS.parent_email = SEASON_1718.parent_email
# LEFT JOIN SEASON_1819 ON ALL_KC_EMAILS.parent_email = SEASON_1819.parent_email
# LEFT JOIN SEASON_1920 ON ALL_KC_EMAILS.parent_email = SEASON_1920.parent_email
# LEFT JOIN SEASON_2021 ON ALL_KC_EMAILS.parent_email = SEASON_2021.parent_email
# LEFT JOIN SEASON_2122 ON ALL_KC_EMAILS.parent_email = SEASON_2122.parent_email
# LEFT JOIN SEASON_2223 ON ALL_KC_EMAILS.parent_email = SEASON_2223.parent_email
# LEFT JOIN custom.ct_customer ON  SEASON_1718.parent_email = ct_customer.email_address
# where ALL_KC_EMAILS.season = '2022-23'
# order by parent_email desc;
# """

# q = """
# select email, transaction_date, add_date
# from custom.kids_club_combined_data
# left join custom.ct_customer on kids_club_combined_data.email = ct_customer.email_address"""

# df = FLA_Redshift(**get_redshift_credentials()).query_warehouse(sql_string=q)

# df['add_date'].fillna(datetime.strptime('2030-01-01', "%Y-%m-%d"), inplace = True)
# df['add_date'] = pd.to_datetime(df['add_date']).dt.date

# df['pre_KC_email'] = df.apply(lambda row: 0 if row['transaction_date'] == '' else (1 if row['add_date'] < datetime.date(datetime.strptime(row['transaction_date'], "%Y-%m-%d")) else 0), axis = 1)
# df['post_KC_email'] = df.apply(lambda row: 0 if row['transaction_date'] == '' else (1 if row['add_date'] >= datetime.date(datetime.strptime(row['transaction_date'], "%Y-%m-%d")) else 0), axis = 1)

# #df = df[df['kc_group'] == 'All-Star']
# print(df)
# print(df['pre_KC_email'].sum())
# print(df['post_KC_email'].sum())
q = """
select email, transaction_date, add_date
from custom.kids_club_combined_data
left join custom.ct_customer on kids_club_combined_data.email = ct_customer.email_address"""

df = FLA_Redshift(**get_redshift_credentials()).query_warehouse(sql_string=q)

df['add_date'].fillna(datetime.strptime('2030-01-01', "%Y-%m-%d"), inplace = True)
df['add_date'] = pd.to_datetime(df['add_date']).dt.date

df['pre_KC_email'] = df.apply(lambda row: 0 if row['transaction_date'] == '' else (1 if row['add_date'] < datetime.date(datetime.strptime(row['transaction_date'], "%Y-%m-%d")) else 0), axis = 1)
df['post_KC_email'] = df.apply(lambda row: 0 if row['transaction_date'] == '' else (1 if row['add_date'] >= datetime.date(datetime.strptime(row['transaction_date'], "%Y-%m-%d")) else 0), axis = 1)

#df = df[df['kc_group'] == 'All-Star']
print(df)
print(df['pre_KC_email'].sum())
print(df['post_KC_email'].sum())


#df.to_csv('C:\\Users\\riffere\\Desktop\\kc_final.csv')
