import cx_Oracle
import numpy as np
import csv
from datetime import datetime
import time

# Point the directory to Oracle Client files
time_stamp = datetime.now().strftime('%Y%m%d')
output_file_path = "C://distribution_dashboard//data//inputs//pos_sales//"
output_file_name = time_stamp + '_' + 'pos_sales.txt'
cx_Oracle.init_oracle_client(lib_dir='C:/distribution_dashboard/src/python/etl/instantclient-basic-windows.x64-21.3.0.0.0/instantclient_21_3')

dsn = cx_Oracle.makedsn(
    'gmdx.tdk.biz',
    '1521',
    service_name='gmds_srv'
)
conn = cx_Oracle.connect(
    user='ORA_DM',
    password='B4u9Srj4h',
    dsn=dsn,
    encoding = 'iso-8859-1')

cursor = conn.cursor()
session = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDD'"
nls = cursor.execute(session)
csv_file = open(output_file_path + output_file_name , "w")
writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
begin = time.time()

# Executing the SQL query
cursor.execute('''
               SELECT
    A.BASE_MONTH, 
    A.ITEM_CODE, 
    A.INVOICE_DATE ,
    A.SITE_CUSTOMER_NO, 
    A.OVS_CUST_NAME_E, 
    A.DIV_SUB, 
    A.DIV_SUB_NAME_E, 
    A.PLAN_GRP, 
    A.PLAN_GRP_NAME_E, 
    A.PLAN_SUM, 
    A.PLAN_SUM_NAME_E, 
    NVL(E.END_CUST_NAME_CL,A.END_CUST_NAME), 
    A.END_CUST_COUNTRY, 
    A.END_CUST_POST,
    SUM(A.SALES_QTY),
    SUM(A.SALES_AMT_ACC*RB.RATE),
    CASE WHEN SUM(A.SALES_QTY) = 0 THEN 0 ELSE SUM(A.SALES_AMT_ACC*RB.RATE)/SUM(A.SALES_QTY) END,
    SUM(NVL2(A.ITEM_CODE_GMDS,A.SALES_QTY,0)),
    SUM(NVL2(A.ITEM_CODE_GMDS,A.SALES_AMT_ACC,0)*RB.RATE),
    CASE WHEN SUM((NVL(A.SALES_QTY_GMDS_P1,0)+NVL(A.SALES_QTY_GMDS_P2,0)+NVL(A.SALES_QTY_GMDS_P3,0)+NVL(A.SALES_QTY_GMDS_P4,0)+NVL(A.SALES_QTY_GMDS_P5,0)+NVL(A.SALES_QTY_GMDS_P6,0)+NVL(A.SALES_QTY_GMDS_P7,0)+NVL(A.SALES_QTY_GMDS_P8,0)+NVL(A.SALES_QTY_GMDS_P9,0)+NVL(A.SALES_QTY_GMDS_P10,0)+NVL(A.SALES_QTY_GMDS_P11,0)+NVL(A.SALES_QTY_GMDS_P12,0))) = 0 THEN 0 ELSE SUM((NVL(A.SALES_AMT_ACC_GMDS_P1 * RA.R01,0)+NVL(A.SALES_AMT_ACC_GMDS_P2 * RA.R02,0)+NVL(A.SALES_AMT_ACC_GMDS_P3 * RA.R03,0)+NVL(A.SALES_AMT_ACC_GMDS_P4 * RA.R04,0)+NVL(A.SALES_AMT_ACC_GMDS_P5 * RA.R05,0)+NVL(A.SALES_AMT_ACC_GMDS_P6 * RA.R06,0)+NVL(A.SALES_AMT_ACC_GMDS_P7 * RA.R07,0)+NVL(A.SALES_AMT_ACC_GMDS_P8 * RA.R08,0)+NVL(A.SALES_AMT_ACC_GMDS_P9 * RA.R09,0)+NVL(A.SALES_AMT_ACC_GMDS_P10 * RA.R10,0)+NVL(A.SALES_AMT_ACC_GMDS_P11 * RA.R11,0)+NVL(A.SALES_AMT_ACC_GMDS_P12 * RA.R12,0)))/SUM((NVL(A.SALES_QTY_GMDS_P1,0)+NVL(A.SALES_QTY_GMDS_P2,0)+NVL(A.SALES_QTY_GMDS_P3,0)+NVL(A.SALES_QTY_GMDS_P4,0)+NVL(A.SALES_QTY_GMDS_P5,0)+NVL(A.SALES_QTY_GMDS_P6,0)+NVL(A.SALES_QTY_GMDS_P7,0)+NVL(A.SALES_QTY_GMDS_P8,0)+NVL(A.SALES_QTY_GMDS_P9,0)+NVL(A.SALES_QTY_GMDS_P10,0)+NVL(A.SALES_QTY_GMDS_P11,0)+NVL(A.SALES_QTY_GMDS_P12,0))) END,
    SUM(A.MARGIN_AMT_ACC * RB.RATE),
    CASE WHEN SUM(NVL2(A.ITEM_CODE_GMDS,A.SALES_AMT_ACC,0)*RB.RATE) = 0 THEN 0 ELSE SUM(A.MARGIN_AMT_ACC * RB.RATE) / SUM(NVL2(A.ITEM_CODE_GMDS,A.SALES_AMT_ACC,0)*RB.RATE) END,
    SUM((NVL(A.SALES_AMT_ACC_GMDS_P1 * RA.R01,0)+NVL(A.SALES_AMT_ACC_GMDS_P2 * RA.R02,0)+NVL(A.SALES_AMT_ACC_GMDS_P3 * RA.R03,0)+NVL(A.SALES_AMT_ACC_GMDS_P4 * RA.R04,0)+NVL(A.SALES_AMT_ACC_GMDS_P5 * RA.R05,0)+NVL(A.SALES_AMT_ACC_GMDS_P6 * RA.R06,0)+NVL(A.SALES_AMT_ACC_GMDS_P7 * RA.R07,0)+NVL(A.SALES_AMT_ACC_GMDS_P8 * RA.R08,0)+NVL(A.SALES_AMT_ACC_GMDS_P9 * RA.R09,0)+NVL(A.SALES_AMT_ACC_GMDS_P10 * RA.R10,0)+NVL(A.SALES_AMT_ACC_GMDS_P11 * RA.R11,0)+NVL(A.SALES_AMT_ACC_GMDS_P12 * RA.R12,0))),
    SUM((NVL(A.SALES_QTY_GMDS_P1,0)+NVL(A.SALES_QTY_GMDS_P2,0)+NVL(A.SALES_QTY_GMDS_P3,0)+NVL(A.SALES_QTY_GMDS_P4,0)+NVL(A.SALES_QTY_GMDS_P5,0)+NVL(A.SALES_QTY_GMDS_P6,0)+NVL(A.SALES_QTY_GMDS_P7,0)+NVL(A.SALES_QTY_GMDS_P8,0)+NVL(A.SALES_QTY_GMDS_P9,0)+NVL(A.SALES_QTY_GMDS_P10,0)+NVL(A.SALES_QTY_GMDS_P11,0)+NVL(A.SALES_QTY_GMDS_P12,0)))
    FROM
    DISTI A,
    EAG_PRODUCT_HIERARCHY_V B,
    END_CUST_COUNTRY C,
    (SELECT
        REGION3,
        MAX(REGION3_NAME_E) REGION3_NAME_E
    FROM
        REGION
    WHERE
        STATUS_CODE IS NULL
    GROUP BY
        REGION3
    ) D,
    END_CUST_INDUSTRY E,
    MARKETING_FIELD F,
    MARKET_GROUP G,
    EXR_BASE_V RA,
    EXRDAY RB
WHERE
    A.FGR = B.FGR (+) AND
    A.END_CUST_COUNTRY = C.END_CUST_COUNTRY (+) AND
    C.REGION3_ALLOC = D.REGION3(+) AND
    A.END_CUST_NAME = E.END_CUST_NAME(+) AND
    NVL(E.END_CUST_INDUSTRY_SEGMENT,'ZO') = F.MARKETING_FIELD(+) AND
    F.MARKET_GROUP = G.MARKET_GROUP(+) AND
    A.BASE_MONTH = RA.BASE_MONTH AND
    A.SALES_CURRENCY_A_GMDS = RA.CURR_FROM(+) AND
    RA.CURR_TO(+) = 'X' AND
    RA.RATE_TYPE(+) = 'A' AND
    A.INVOICE_DATE = RB.BASE_DATE(+) AND
    A.CURRENCY_A = RB.CURR_FROM(+) AND
    A.BASE_MONTH >= 201904 AND
    A.DATA_TYPE = 'SALES' AND
    RB.CURR_TO(+) = 'EUR'
GROUP BY
    A.BASE_MONTH,
    A.ITEM_CODE,
    A.INVOICE_DATE,
    A.SITE_CUSTOMER_NO,
    A.OVS_CUST_NAME_E,
    A.DIV_SUB,
    A.DIV_SUB_NAME_E,
    A.PLAN_GRP,
    A.PLAN_GRP_NAME_E,
    A.PLAN_SUM,
    A.PLAN_SUM_NAME_E,
    NVL(E.END_CUST_NAME_CL,A.END_CUST_NAME),
    A.END_CUST_COUNTRY,
    A.END_CUST_POST
ORDER BY
    A.BASE_MONTH,
    A.INVOICE_DATE,
    A.SITE_CUSTOMER_NO,
    A.OVS_CUST_NAME_E,
    A.DIV_SUB,
    A.DIV_SUB_NAME_E,
    A.PLAN_GRP,
    A.PLAN_GRP_NAME_E,
    A.PLAN_SUM,
    A.PLAN_SUM_NAME_E,
    A.ITEM_CODE,
    NVL(E.END_CUST_NAME_CL,A.END_CUST_NAME),
    A.END_CUST_COUNTRY,
    A.END_CUST_POST
               ''')

# this stores the column names
print("Authenticated with the Oracle SQL server and initiating querying...")

col_names = [row[0].lower() for row in cursor.description]
writer.writerow(col_names)
i = 0
# reading and writing the first ten columns
for row in cursor:
    i += 1
    print("The number of rows written", i)
    writer.writerow(row)

cursor.close()
# conn.close()
csv_file.close()
# store ending time
end = time.time()

# total time taken
print(f"Total runtime of the program is {np.round((end - begin)/60,2)} minutes")

