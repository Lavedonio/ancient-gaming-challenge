"""
# Ancient Gaming Challenge DAG
This DAG handles the ELT proccess for the challenge.

## Pipeline structure
The pipeline contains 6 stages, each separated in the Aiflow DAG by their own Task Group. They are:

1. Pre loading
2. Level 1 - landing
3. Level 2 - source
4. Level 3 - intermediate
5. Level 4 - final
6. Level 5 - consumption

### Pre loading
This part takes care of creating the data in the raw files and uploading them to Google Cloud Composer,
where they will be loaded into BigQuery in the next step.

This is the part that would be the most diferent in a real production environment. In that case,
the files wouldn't be generated on the spot, but rather be added through a different service,
or even added to BigQuery directly via Fivetran or Stitch, for instance, skipping this process altogether.

### Level 1 = landing
This is the first stage of the data in BigQuery. Here, the raw data is loaded as is and partitioned
by the execution date that comes from Airflow. This part is crucial to make sure that the pipeline is
idempotent and can be backfilled in the future if needed.

### Level 2 - source
This is the second stage of the data in BigQuery. Here, the raw data from the previous stage is cleaned
so that this will be the first clean slate for the following processing steps.

### Level 3 - intermediate
This is the third stage of the data in BigQuery. Here, the source data is processed and intermediate tables
are created to be used in multiple final tables or to make it easier to maintain a more complex logic by
dividing it in multiple stages. This is not a necessary step and some data flows go directly from source to final.

### Level 4 - final
This is the forth stage of the data in BigQuery. Here, the final queries are created to then feed both
reports and dashboards that will be used by the Data Analysts or be consumed by other services.

### Level 5 - consumption
This is the fifth and final stage of the data in BigQuery. Here, the there's no extra processing
and all the final tables are presented here by simply selecting all fields. The reason for this stage to be present
is to be the target for all further processing and being the connection point for data visualization tools.
That way, if there's a data source migration in the future, this stage can behave as a valve to which data stream
goes to the reports, making sure the Data Engineers can control the flow and make adjustments if necessary without
any extra work from our stakeholders downstream.
"""

import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup

from custom_operators.dbt import DBTRunOperator
from scripts.generate_raw_data import generate_raw_data, remove_files

class TablesAux:

    def __init__(self, name: str):
        self.name = name

        self.pre_loading_upload_gcs_task = None
        self.pre_loading_remove_local_task = None
        self.landing_task = None
        self.source_task = None

    def create_dependencies(self):
        (
            self.pre_loading_upload_gcs_task
            >> self.pre_loading_remove_local_task
            >> self.landing_task
            >> self.source_task
        )


RAW_TABLES = {
    'users': TablesAux('users'),
    'user_preferences': TablesAux('user_preferences'),
    'transactions': TablesAux('transactions'),
}


with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Ancient Challenge",
    start_date=datetime.datetime(2024, 12, 11),
    schedule="0 6 * * *",
    description="This DAG handles the ELT proccess for the challenge",
    doc_md=__doc__,
    # Since it's a weak machine I've put in Composer,
    # the amount of concurrent tasks needs to be limited.
    max_active_runs=1,
    max_active_tasks=2,
):
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    with TaskGroup(group_id='pre_loading') as pre_loading:
        generate_raw_data_task = PythonOperator(
            task_id='generate_raw_data',
            python_callable=generate_raw_data,
            op_args=[True],
        )

        for table in RAW_TABLES.values():
            upload_file_to_bq_task = LocalFilesystemToGCSOperator(
                task_id=f'upload_file_{table.name}_to_gcs',
                src=f'raw_{table.name}.csv',
                dst="".join([
                    'challenge_data/',
                    f'{table.name}/',
                    'ds={{ ds }}/',
                    'file.csv'
                ]),
                bucket='ancient-challenge-lavedonio'
            )
            table.pre_loading_upload_gcs_task = upload_file_to_bq_task

            remove_file_task = PythonOperator(
                task_id=f'remove_file_{table.name}',
                python_callable=remove_files,
                op_args=[f'raw_{table.name}.csv']
            )
            table.pre_loading_remove_local_task = remove_file_task

            generate_raw_data_task >> upload_file_to_bq_task

    with TaskGroup(group_id='level1_landing') as level1_landing:

        for table in RAW_TABLES.values():
            landing_raw = GCSToBigQueryOperator(
                task_id=f"landing_raw_{table.name}",
                bucket='ancient-challenge-lavedonio',
                source_objects=f'challenge_data/raw_{table.name}/*.csv',
                project_id='stoked-courier-444606-c2',
                destination_project_dataset_table=f'l1_landing.raw_{table.name}',
                create_disposition='CREATE_NEVER',
                write_disposition='WRITE_APPEND',
                time_partitioning={'field': 'ds', 'type': 'DAY'},
                autodetect=True,
                skip_leading_rows=1
            )
            table.landing_task = landing_raw

    with TaskGroup(group_id='level2_source') as level2_source:
        for table in RAW_TABLES.values():
            dbt_run_source_task = DBTRunOperator(
                task_id=f"dbt_run_source_{table.name}",
                model=f"challenge.l2_source.{table.name}",
            )
            table.source_task = dbt_run_source_task

    with TaskGroup(group_id='level3_intermediate') as level3_intermediate:
        dbt_run_intermediate_helper_user_daily_transactions_task = DBTRunOperator(
            task_id="dbt_run_intermediate_helper_user_daily_transactions",
            model="challenge.l3_intermediate.helper_user_daily_transactions",
        )

        dbt_run_intermediate_user_preferences_extra_info_task = DBTRunOperator(
            task_id="dbt_run_intermediate_user_preferences_extra_info",
            model="challenge.l3_intermediate.user_preferences_extra_info",
        )

    with TaskGroup(group_id='level4_final') as level4_final:
        dbt_run_final_new_users_last_30days_task = DBTRunOperator(
            task_id="dbt_run_final_new_users_last_30days",
            model="challenge.level4_final.new_users_last_30days",
        )

        dbt_run_final_user_activity_summary_task = DBTRunOperator(
            task_id="dbt_run_final_user_activity_summary",
            model="challenge.level4_final.user_activity_summary",
        )

        dbt_run_final_user_daily_transactions_task = DBTRunOperator(
            task_id="dbt_run_final_user_daily_transactions",
            model="challenge.level4_final.user_daily_transactions",
        )

        dbt_run_final_user_latest_preferences_task = DBTRunOperator(
            task_id="dbt_run_final_user_latest_preferences",
            model="challenge.level4_final.user_latest_preferences",
        )

    with TaskGroup(group_id='level5_consumption') as level5_consumption:
        dbt_run_consumption_report_new_users_last_30days_task = DBTRunOperator(
            task_id="dbt_run_consumption_report_new_users_last_30days",
            model="challenge.level5_consumption.report_new_users_last_30days",
        )

        dbt_run_consumption_report_user_activity_summary_task = DBTRunOperator(
            task_id="dbt_run_consumption_report_user_activity_summary",
            model="challenge.level5_consumption.report_user_activity_summary",
        )

        dbt_run_consumption_report_user_daily_transactions_task = DBTRunOperator(
            task_id="dbt_run_consumption_report_user_daily_transactions",
            model="challenge.level5_consumption.report_user_daily_transactions",
        )

        dbt_run_consumption_report_user_latest_preferences_task = DBTRunOperator(
            task_id="dbt_run_consumption_report_user_latest_preferences",
            model="challenge.level5_consumption.report_user_latest_preferences",
        )

    ## Dependencies
    start_task >> generate_raw_data_task

    pre_loading >> level1_landing

    # Pre Loading to Level 1 to Level 2
    for table in RAW_TABLES.values():
        table.create_dependencies()

    # Level 2 to Level 3
    RAW_TABLES['transactions'].source_task >> dbt_run_intermediate_helper_user_daily_transactions_task
    RAW_TABLES['user_preferences'].source_task >> dbt_run_intermediate_user_preferences_extra_info_task

    # Levels 2 and 3 to Level 4
    [
        RAW_TABLES['transactions'].source_task,
        RAW_TABLES['users'].source_task
    ] >> dbt_run_final_new_users_last_30days_task
    [
        dbt_run_intermediate_helper_user_daily_transactions_task,
        dbt_run_intermediate_user_preferences_extra_info_task
    ] >> dbt_run_final_user_activity_summary_task
    dbt_run_intermediate_helper_user_daily_transactions_task >> dbt_run_final_user_daily_transactions_task
    [
        RAW_TABLES['users'].source_task,
        dbt_run_intermediate_user_preferences_extra_info_task
    ] >> dbt_run_final_user_latest_preferences_task

    # Level 4 to Level 5
    dbt_run_final_new_users_last_30days_task >> dbt_run_consumption_report_new_users_last_30days_task >> end_task
    dbt_run_final_user_activity_summary_task >> dbt_run_consumption_report_user_activity_summary_task >> end_task
    dbt_run_final_user_daily_transactions_task >> dbt_run_consumption_report_user_daily_transactions_task >> end_task
    dbt_run_final_user_latest_preferences_task >> dbt_run_consumption_report_user_latest_preferences_task >> end_task
