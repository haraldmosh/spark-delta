from datetime import timedelta
import airflow
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

# Custom Operators deployed as Airflow plugins
from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
from awsairflowlib.operators.aws_glue_crawler_operator import AWSGlueCrawlerOperator
from awsairflowlib.operators.aws_copy_s3_to_redshift import CopyS3ToRedshiftOperator

S3_BUCKET_NAME = "airflow-resources-store"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'delta_data_pipeline',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

s3_sensor = S3PrefixSensor(
    task_id='s3_sensor',
    bucket_name=S3_BUCKET_NAME,
    prefix='data/bronze',
    dag=dag
)

glue_crawler = AWSGlueCrawlerOperator(
    task_id="glue_crawler",
    crawler_name='airflow-security-bronze-crawler',
    iam_role_name='AWSGlueServiceRoleDefault',
    dag=dag
)

glue_task = AWSGlueJobOperator(
    task_id="glue_task",
    job_name='nyc_raw_to_transform',
    iam_role_name='AWSGlueServiceRoleDefault',
    dag=dag
)


# EMR

execution_date = "{{ execution_date }}"

EMR_S3_URI = f"s3://{S3_BUCKET_NAME}/scripts/emr/"

JOB_FLOW_OVERRIDES = {
    "Name": "Data-Pipeline-" + execution_date,
    "ReleaseLabel": "emr-6.3.1",
    "LogUri": "s3://{}/logs/emr/".format(S3_BUCKET_NAME),
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": True
    },
    "BootstrapActions": [
        {
            "Name": "string",
            "ScriptBootstrapAction": {
                "Path": f"{EMR_S3_URI}emr_bootstrap.sh",
            }
        },
    ],
}

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)


SPARK_TEST_STEPS = [
    {
        'Name': 'setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', '--recursive', EMR_S3_URI, '/home/hadoop/']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '/home/hadoop/nyc_aggregations.py',
                f's3://{S3_BUCKET_NAME}/data/silver',
                f's3://{S3_BUCKET_NAME}/data/gold'
            ]
        }
    }
]

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

s3_sensor >> glue_crawler >> glue_task >> cluster_creator >> step_adder >> step_checker >> cluster_remover
