import cx_Oracle
import numpy as np
import csv
from datetime import datetime
import time

# Point the directory to Oracle Client files
#time_stamp = datetime.now().strftime('%Y%m%d')
time_stamp = '20221029'
output_file_path = "C://distribution_dashboard//data//inputs//gmds_sales//"
output_file_name = time_stamp + '_' + 'gmds_sales.txt'
cx_Oracle.init_oracle_client(lib_dir='C:/distribution_dashboard/src/python/etl/instantclient-basic-windows.x64-21.3.0.0.0/instantclient_21_3')

dsn = cx_Oracle.makedsn(
    'gmdx.tdk.biz',
    '1521',
    service_name='gmds_srv'
)
conn = cx_Oracle.connect(
    user='ORA_DM',
    password='B4u9Srj4h',
    dsn=dsn
)
cursor = conn.cursor()
csv_file = open(output_file_path + output_file_name , "w")
writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
begin = time.time()

# Executing the SQL query

cursor.execute('''
SELECT 
    a.base_month, 
    a.process_date, 
    a.term,
    a.brand,
    a.div_sub, 
    a.div_sub_name_e, 
    a.plan_grp, 
    a.plan_grp_name_e, 
    a.plan_sum, 
    a.plan_sum_name_e, 
    a.plan_itgrp, 
    a.plan_itgrp_name_e, 
    a.item_code, 
    a.item_name, 
    a.sao_group, 
    a.sao_group_name_e, 
    a.ovs_cust_name, 
    a.sap_customer_no, 
    a.disty_type, 
    a.disty_type_name, 
    a.budget_amt, 
    a.budget_qty, 
    a.budget_price, 
    a.result_amt, 
    a.result_qty,
    a.result_price, 
    a.co_amt, 
    a.co_qty, 
    a.co_price, 
    a.fi_amt, 
    a.fi_qty, 
    a.fi_price,
    a.bok_amt,
    a.bok_qty,
    a.bok_price,
    a.country_code,
    a.zip_code,
    a.region1,
    a.region1_name_e,
    a.region3,
    a.region3_name_e,
    a.region5,
    a.region5_name_e,
    a.territory_code,
    a.territory_name,
    a.district_code,
    a.district_name
FROM(
SELECT
    A.BASE_MONTH,
    A.TERM,
    A.BRAND,
    A.BASE_MONTH * 100 + 1 AS PROCESS_DATE,
    A.DIV_SUB,
    DIVSB.DIV_SUB_NAME_E,
    A.PLAN_GRP,
    PLNGRP.PLAN_GRP_NAME_E,
    A.PLAN_SUM,
    PLNSUM.PLAN_SUM_NAME_E,
    A.PLAN_ITGRP,
    PLNIT.PLAN_ITGRP_NAME_E,
    '' as item_code,
    '' as item_name,
    A.SAO_GROUP,
    SAO_GROUP.SAO_GROUP_NAME_E,
    A.CUST_GROUP,
    CUSTOMER_GROUP.CUSTOMER_GRP_NAME_E,
    A.SAP_CUSTOMER_NO,
    A.OVS_CUST_NO,
    CASE WHEN A.SALES_LOCATION IN ('TDK') THEN CUSTOMER.CUSTOMER_NAME_E ELSE OSISCUSTV.COSMOS_CUST_NAME END as ovs_cust_name,
    A.DISTY_TYPE,
    DISTY_TYPE.DISTY_TYPE_NAME,
    SUM(NVL(A.BUDGET_AMT,0)) as budget_amt,
    SUM(NVL(A.BUDGET_QTY,0)) as budget_qty,
    SUM(NVL(A.BUDGET_AMT,0)) / NULLIF(SUM(NVL(A.BUDGET_QTY,0)),0) as budget_price,
    0 as result_amt,
    0 as result_qty,
    0 as result_price,
    0 as co_amt,
    0 as co_qty,
    0 as co_price,
    0 as fi_amt,
    0 as fi_qty,
    0 as fi_price,
    0 as bok_amt,
    0 as bok_qty,
    0 as bok_price,
    OSISCUSTV.COUNTRY_CODE,
    OSISCUSTV.ZIP_CODE,
    REGION.REGION1,
    REGION.REGION1_NAME_E,
    REGION.REGION3,
    REGION.REGION3_NAME_E,
    OSISCUSTV.REGION5,
    REGION.REGION5_NAME_E,
    OSISCUSTV.TERRITORY_CODE,
    OSISCUSTV.TERRITORY_NAME,
    OSISCUSTV.DISTRICT_CODE,
    OSISCUSTV.DISTRICT_NAME
FROM
    GBUDGET_BASE_MONTHLY_V_X A,
    OSIS_CUSTOMER_V OSISCUSTV,
    CUSTOMER,
    SAO_GROUP,
    CUSTOMER_GROUP,
    DISTY_TYPE,
    DIVSB,
    PLNGRP,
    PLNSUM,
    PLNIT,
    REGION
WHERE
    (A.BASE_MONTH >= 202004 AND A.BASE_MONTH <=202412)  AND
    A.BRAND IN ('EPCOS','TDK') AND
--    A.REGION1 IN ('7') AND
    (A.V_EPCOS_CONS_FLAG = 'Y' and A.PLANT_SALES = '0') AND
    A.SALES_LOCATION = OSISCUSTV.LOCATION(+) AND
    A.OVS_CUST_NO = OSISCUSTV.COSMOS_CUST_NO(+) AND
    A.OVS_CUST_NO = CUSTOMER.CUSTOMER_NO(+) AND
    A.SAO_GROUP = SAO_GROUP.SAO_GROUP(+) AND
    A.CUST_GROUP = CUSTOMER_GROUP.CUSTOMER_GRP_CODE(+) AND
    A.DISTY_TYPE = DISTY_TYPE.DISTY_TYPE(+) AND
    (A.DIV_SUB = DIVSB.DIV_SUB_CODE(+) AND (DIVSB.STATUS_CODE <> 'D' OR DIVSB.STATUS_CODE IS NULL))  AND
    A.PLAN_GRP = PLNGRP.PLAN_GRP(+) AND
    A.PLAN_SUM = PLNSUM.PLAN_SUM(+) AND
    A.PLAN_ITGRP = PLNIT.PLAN_ITEM_GROUP(+) AND
    OSISCUSTV.REGION5 = REGION.REGION5(+)
GROUP BY
    A.BASE_MONTH,
    A.TERM,
    A.BRAND,
    A.DIV_SUB,
    DIVSB.DIV_SUB_NAME_E,
    A.PLAN_GRP,
    PLNGRP.PLAN_GRP_NAME_E,
    A.PLAN_SUM,
    PLNSUM.PLAN_SUM_NAME_E,
    A.PLAN_ITGRP,
    PLNIT.PLAN_ITGRP_NAME_E,
    A.SAO_GROUP,
    SAO_GROUP.SAO_GROUP_NAME_E,
    A.CUST_GROUP,
    CUSTOMER_GROUP.CUSTOMER_GRP_NAME_E,
    A.SAP_CUSTOMER_NO,
    A.OVS_CUST_NO,
    CASE WHEN A.SALES_LOCATION IN ('TDK') THEN CUSTOMER.CUSTOMER_NAME_E ELSE OSISCUSTV.COSMOS_CUST_NAME END,
    A.DISTY_TYPE,
    DISTY_TYPE.DISTY_TYPE_NAME,
    OSISCUSTV.COUNTRY_CODE,
    OSISCUSTV.ZIP_CODE,
    REGION.REGION1,
    REGION.REGION1_NAME_E,
    REGION.REGION3,
    REGION.REGION3_NAME_E,
    OSISCUSTV.REGION5,
    REGION.REGION5_NAME_E,
    OSISCUSTV.TERRITORY_CODE,
    OSISCUSTV.TERRITORY_NAME,
    OSISCUSTV.DISTRICT_CODE,
    OSISCUSTV.DISTRICT_NAME
HAVING
    SUM(A.BUDGET_AMT) <> 0 OR
    SUM(A.BUDGET_QTY) <> 0 OR
    SUM(A.BUDGET_AMT) / NULLIF(SUM(A.BUDGET_QTY),0) <> 0
UNION ALL
SELECT
    A.BASE_MONTH,
    A.TERM,
    A.BRAND,
    A.DAY,
    A.DIV_SUB,
    DIVSB.DIV_SUB_NAME_E,
    A.PLAN_GRP,
    PLNGRP.PLAN_GRP_NAME_E,
    A.PLAN_SUM,
    PLNSUM.PLAN_SUM_NAME_E,
    A.PLAN_ITGRP,
    PLNIT.PLAN_ITGRP_NAME_E,
    A.DIVISION || A.ITEM_CODE,
    A.ITEM_NAME,
    A.SAO_GROUP,
    SAO_GROUP.SAO_GROUP_NAME_E,
    A.CUST_GROUP,
    CUSTOMER_GROUP.CUSTOMER_GRP_NAME_E,
    A.SAP_CUSTOMER_NO,
    A.OVS_CUST_NO,
    CASE WHEN A.SALES_LOCATION IN ('TDK') THEN CUSTOMER.CUSTOMER_NAME_E ELSE OSISCUSTV.COSMOS_CUST_NAME END,
    A.DISTY_TYPE,
    DISTY_TYPE.DISTY_TYPE_NAME,
    0,
    0,
    0,
    SUM(A.AMT2),
    SUM(A.QTY2),
    SUM(A.AMT2) / NULLIF(SUM(A.QTY2),0),
    SUM(A.AMT4),
    SUM(A.QTY4),
    SUM(A.AMT4) / NULLIF(SUM(A.QTY4),0),
    SUM(A.AMT6),
    SUM(A.QTY6),
    SUM(A.AMT6) / NULLIF(SUM(A.QTY6),0),
    SUM(A.AMT7),
    SUM(A.QTY7),
    SUM(A.AMT7) / NULLIF(SUM(A.QTY7),0),
    OSISCUSTV.COUNTRY_CODE,
    OSISCUSTV.ZIP_CODE,
    REGION.REGION1,
    REGION.REGION1_NAME_E,
    REGION.REGION3,
    REGION.REGION3_NAME_E,
        OSISCUSTV.REGION5,
    REGION.REGION5_NAME_E,
    OSISCUSTV.TERRITORY_CODE,
    OSISCUSTV.TERRITORY_NAME,
    OSISCUSTV.DISTRICT_CODE,
    OSISCUSTV.DISTRICT_NAME
FROM
    (
     SELECT
        A.BASE_MONTH,
        A.TERM,
        A.BRAND,
        A.PROCESS_DATE as DAY,
        A.DIV_SUB,
        A.PLAN_GRP,
        A.PLAN_SUM,
        A.PLAN_ITGRP,
        A.DIVISION,
        A.ITEM_CODE,
        A.ITEM_NAME,
        A.SAO_GROUP,
        A.CUST_GROUP,
        A.SAP_CUSTOMER_NO,
        A.DISTY_TYPE,
        A.OVS_CUST_NO,
        A.SALES_LOCATION,
        A.MMT_OVS_CUST_NO,
        NVL(A.SLS_AMT,0) as AMT2,
        NVL(A.SLS_QTY,0) as QTY2,
        0 as PRICE2,
        0 as AMT4,
        0 as QTY4,
        0 as PRICE4,
        0 as AMT6,
        0 as QTY6,
        0 as PRICE6,
        0 as AMT7,
        0 as QTY7,
        0 as PRICE7
    FROM
        GISLS_PROCESS_DAILY_V_X A
    WHERE
        (A.PROCESS_DATE >= 20200401 AND  A.PROCESS_DATE <= 20241231) AND
        A.BRAND IN ('EPCOS','TDK') AND
        (A.BASE_MONTH >= 202004 AND A.BASE_MONTH <= 202412) AND
        (A.V_EPCOS_CONS_FLAG = 'Y' and A.PLANT_SALES = '0')
    UNION ALL 
     SELECT
        A.BASE_MONTH,
        A.TERM,
        A.BRAND,
        A.PROCESS_DATE as DAY,
        A.DIV_SUB,
        A.PLAN_GRP,
        A.PLAN_SUM,
        A.PLAN_ITGRP,
        A.DIVISION,
        A.ITEM_CODE,
        A.ITEM_NAME,
        A.SAO_GROUP,
        A.CUST_GROUP,
        A.SAP_CUSTOMER_NO,
        A.DISTY_TYPE,
        A.OVS_CUST_NO,
        A.SALES_LOCATION,
        A.MMT_OVS_CUST_NO,
        0 as AMT2,
        0 as QTY2,
        0 as PRICE2,
        0 as AMT4,
        0 as QTY4,
        0 as PRICE4,
        0 as AMT6,
        0 as QTY6,
        0 as PRICE6,
        NVL(A.BOK_AMT,0) as AMT7,
        NVL(A.BOK_QTY,0) as QTY7,
        0 as PRICE7
    FROM
        GIBB_PROCESS_DAILY_V_X A
    WHERE
        (A.PROCESS_DATE >= 2020401 AND  A.PROCESS_DATE <= 20241231) AND
        A.BRAND IN ('EPCOS','TDK') AND
        (A.BASE_MONTH >= 202004 AND A.BASE_MONTH <=202412) AND
--        A.REGION1 IN ('7') AND
        (A.V_EPCOS_CONS_FLAG = 'Y' and A.PLANT_SALES = '0') AND
        (NVL(A.TR_CODE,'X') != '37')
    UNION ALL 
    SELECT
        TO_NUMBER(SUBSTR(A.BASE_DATE,1,6)),
        A.TERM,
        A.BRAND,
        A.BASE_DATE,
        A.DIV_SUB,
        A.PLAN_GRP,
        A.PLAN_SUM,
        A.PLAN_ITGRP,
        A.DIVISION,
        A.ITEM_CODE,
        A.ITEM_NAME,
        A.SAO_GROUP,
        A.CUST_GROUP,
        A.SAP_CUSTOMER_NO,
        A.DISTY_TYPE,
        A.OVS_CUST_NO,
        A.SALES_LOCATION,
        A.MMT_OVS_CUST_NO,
        0 as AMT2,
        0 as QTY2,
        0 as PRICE2,
        NVL(A.ORDER_EB_AMT,0) as AMT4,
        NVL(A.ORDER_EB_QTY,0) as QTY4,
        0 as PRICE4,
        NVL(A.FI_EB_AMT,0) as AMT6,
        NVL(A.FI_EB_QTY,0) as QTY6,
        0 as PRICE6,
        0 as AMT7,
        0 as QTY7,
        0 as PRICE7
    FROM
        GIORDER_BASE_DAILY_V_X A
    WHERE
--        A.REGION1 IN ('7') AND
        A.BRAND IN ('EPCOS','TDK') AND
        (A.V_EPCOS_CONS_FLAG = 'Y' and A.PLANT_SALES = '0') AND
        (NVL(A.TR_CODE,'X') != '37')
    ) A ,
    OSIS_CUSTOMER_V OSISCUSTV,
    CUSTOMER,
    SAO_GROUP,
    CUSTOMER_GROUP,
    DISTY_TYPE,
    DIVSB,
    PLNGRP,
    PLNSUM,
    PLNIT,
    REGION
WHERE
    REGION.REGION1_NAME_E IN ('EUROPE') AND
    A.BRAND IN ('EPCOS','TDK') AND
    A.SALES_LOCATION = OSISCUSTV.LOCATION(+) AND
    A.OVS_CUST_NO = OSISCUSTV.COSMOS_CUST_NO(+) AND
    A.OVS_CUST_NO = CUSTOMER.CUSTOMER_NO(+) AND
    A.SAO_GROUP = SAO_GROUP.SAO_GROUP(+) AND
    A.CUST_GROUP = CUSTOMER_GROUP.CUSTOMER_GRP_CODE(+) AND
    A.DISTY_TYPE = DISTY_TYPE.DISTY_TYPE(+) AND
    (A.DIV_SUB = DIVSB.DIV_SUB_CODE(+) AND (DIVSB.STATUS_CODE <> 'D' OR DIVSB.STATUS_CODE IS NULL))  AND
    A.PLAN_GRP = PLNGRP.PLAN_GRP(+) AND
    A.PLAN_SUM = PLNSUM.PLAN_SUM(+) AND
    A.PLAN_ITGRP = PLNIT.PLAN_ITEM_GROUP(+) AND
    OSISCUSTV.REGION5 = REGION.REGION5(+) 
GROUP BY
    A.BASE_MONTH,
    A.TERM,
    A.BRAND,
    A.DAY,
    A.DIV_SUB,
    DIVSB.DIV_SUB_NAME_E,
    A.PLAN_GRP,
    PLNGRP.PLAN_GRP_NAME_E,
    A.PLAN_SUM,
    PLNSUM.PLAN_SUM_NAME_E,
    A.PLAN_ITGRP,
    PLNIT.PLAN_ITGRP_NAME_E,
    A.DIVISION || A.ITEM_CODE,
    A.ITEM_NAME,
    A.SAO_GROUP,
    SAO_GROUP.SAO_GROUP_NAME_E,
    A.CUST_GROUP,
    CUSTOMER_GROUP.CUSTOMER_GRP_NAME_E,
    A.SAP_CUSTOMER_NO,
    A.OVS_CUST_NO,
    CASE WHEN A.SALES_LOCATION IN ('TDK') THEN CUSTOMER.CUSTOMER_NAME_E ELSE OSISCUSTV.COSMOS_CUST_NAME END,
    A.DISTY_TYPE,
    DISTY_TYPE.DISTY_TYPE_NAME,
    OSISCUSTV.COUNTRY_CODE,
    OSISCUSTV.ZIP_CODE,
    REGION.REGION1,
    REGION.REGION1_NAME_E,
    REGION.REGION3,
    REGION.REGION3_NAME_E,
    OSISCUSTV.REGION5,
    REGION.REGION5_NAME_E,
    OSISCUSTV.TERRITORY_CODE,
    OSISCUSTV.TERRITORY_NAME,
    OSISCUSTV.DISTRICT_CODE,
    OSISCUSTV.DISTRICT_NAME
HAVING
    SUM(A.AMT2) <> 0 OR
    SUM(A.QTY2) <> 0 OR
    SUM(A.AMT2) / NULLIF(SUM(A.QTY2),0) <> 0 OR
    SUM(A.AMT4) <> 0 OR
    SUM(A.QTY4) <> 0 OR
    SUM(A.AMT4) / NULLIF(SUM(A.QTY4),0) <> 0 OR
    SUM(A.AMT6) <> 0 OR
    SUM(A.QTY6) <> 0 OR
    SUM(A.AMT6) / NULLIF(SUM(A.QTY6),0) <> 0 OR
    SUM(A.AMT7) <> 0 OR
    SUM(A.QTY7) <> 0 OR
    SUM(A.AMT7) / NULLIF(SUM(A.QTY7),0) <> 0
ORDER BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14
) a
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


