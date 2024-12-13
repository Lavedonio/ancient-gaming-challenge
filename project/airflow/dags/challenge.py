"""
# This DAG handles the ELT proccess for the challenge.
"""

import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# from scripts.custom_operators.dbt_operator import DBTOperator
from airflow.operators.bash import BashOperator


class DBTRunOperator(BashOperator):
    """
    This class is a wrapper of the BashOperator to make the DBT runs
    easy to create and standardized.
    """

    def __init__(self, model: str, **kwargs) -> None:
        self.model = model
        kwargs['bash_command'] = f"dbt run --project-dir /opt/airflow/dbt/ancient --profiles-dir /opt/airflow/dbt --select {self.model}"
        super().__init__(**kwargs)

    def execute(self, context):
        super().execute(context)


class DBTTestOperator(BashOperator):
    """
    This class is a wrapper of the BashOperator to make the DBT tests
    easy to create and standardized.
    """

    def __init__(self, model: str, **kwargs) -> None:
        self.model = model
        kwargs['bash_command'] = f"dbt test --project-dir /opt/airflow/dbt/ancient --profiles-dir /opt/airflow/dbt --select {self.model}"
        super().__init__(**kwargs)

    def execute(self, context):
        super().execute(context)


with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Ancient Challenge",
    start_date=datetime.datetime(2024, 12, 11),
    schedule="0 6 * * *",
    description=__doc__
):
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    with TaskGroup(group_id='level1_landing') as level1_landing:
        landing_users = EmptyOperator(task_id="landing_users")
    
    start_task >> level1_landing

    with TaskGroup(group_id='level2_source') as level2_source:
        dbt_run_source_users_task = DBTRunOperator(
            task_id="dbt_run_source_users",
            model="challenge.l2_source.users",
        )

        dbt_run_source_user_preferences_task = DBTRunOperator(
            task_id="dbt_run_source_user_preferences",
            model="challenge.l2_source.user_preferences",
        )

        dbt_run_source_transactions_task = DBTRunOperator(
            task_id="dbt_run_source_transactions",
            model="challenge.l2_source.transactions",
        )

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
    start_task >> level1_landing >> level2_source

    # Level 2 to Level 3
    dbt_run_source_transactions_task >> dbt_run_intermediate_helper_user_daily_transactions_task
    dbt_run_source_user_preferences_task >> dbt_run_intermediate_user_preferences_extra_info_task

    # Levels 2 and 3 to Level 4
    [
        dbt_run_source_transactions_task,
        dbt_run_source_users_task
    ] >> dbt_run_final_new_users_last_30days_task
    [
        dbt_run_intermediate_helper_user_daily_transactions_task,
        dbt_run_intermediate_user_preferences_extra_info_task
    ] >> dbt_run_final_user_activity_summary_task
    dbt_run_intermediate_helper_user_daily_transactions_task >> dbt_run_final_user_daily_transactions_task
    [
        dbt_run_source_users_task,
        dbt_run_intermediate_user_preferences_extra_info_task
    ] >> dbt_run_final_user_latest_preferences_task

    # Level 4 to Level 5
    dbt_run_final_new_users_last_30days_task >> dbt_run_consumption_report_new_users_last_30days_task >> end_task
    dbt_run_final_user_activity_summary_task >> dbt_run_consumption_report_user_activity_summary_task >> end_task
    dbt_run_final_user_daily_transactions_task >> dbt_run_consumption_report_user_daily_transactions_task >> end_task
    dbt_run_final_user_latest_preferences_task >> dbt_run_consumption_report_user_latest_preferences_task >> end_task
