from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'acheryabkin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE acheryabkin.ods_billing PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, cast(concat(billing_period, '-01') as DATE), service, tariff, cast(sum as INT), cast(created_at as DATE) 
        FROM acheryabkin.stg_billing 
        WHERE year(created_at) = '{{ execution_date.year }}';
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE acheryabkin.ods_issue PARTITION (year='{{ execution_date.year }}') 
        SELECT cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service 
        FROM acheryabkin.stg_issue 
        WHERE year(start_time) = '{{ execution_date.year }}';
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE acheryabkin.ods_payment PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, pay_doc_type, pay_doc_num, account, phone, cast(concat(billing_period, '-01') as DATE), cast(pay_date as DATE), cast(sum as DECIMAL(10,2)) 
        FROM acheryabkin.stg_payment 
        WHERE year(pay_date) = '{{ execution_date.year }}';
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE acheryabkin.ods_traffic PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, cast(`timestamp` as TIMESTAMP), device_id, device_ip_addr, bytes_sent, bytes_received 
        FROM acheryabkin.stg_traffic 
        WHERE year(from_unixtime(cast(`timestamp`/1000 as BIGINT))) = '{{ execution_date.year }}';
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dm_bytes_received = DataProcHiveOperator(
    task_id='dm_bytes_received',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE acheryabkin.dm_bytes_received PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, max(bytes_received), min(bytes_received), cast(avg(bytes_received) as INT) 
        FROM acheryabkin.ods_traffic 
        WHERE `year` = '{{ execution_date.year }}' 
        GROUP BY user_id;
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_bytes_received_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_billing, ods_issue, ods_payment, ods_traffic >> dm_bytes_received
