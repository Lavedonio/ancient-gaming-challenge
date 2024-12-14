"""
This module contains all the DBT related custom operators and auxiliary functions.
"""
from pathlib import Path

from airflow.operators.bash import BashOperator


DBT_PATH = Path(__file__).resolve().parents[1] / 'dbt'


class DBTRunOperator(BashOperator):
    """
    This class is a wrapper of the BashOperator to make the DBT runs
    easy to create and standardized.
    """

    def __init__(self, model: str, **kwargs) -> None:
        self.model = model
        dbt_project_path = DBT_PATH / 'ancient'
        kwargs['bash_command'] = f"dbt run --project-dir {dbt_project_path} --profiles-dir {dbt_project_path} --select {self.model}"
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
        dbt_project_path = DBT_PATH / 'ancient'
        kwargs['bash_command'] = f"dbt test --project-dir {dbt_project_path} --profiles-dir {dbt_project_path} --select {self.model}"
        super().__init__(**kwargs)

    def execute(self, context):
        super().execute(context)


def dbt_run_and_test_operators(base_task_id: str, model: str) -> tuple[DBTRunOperator, DBTTestOperator]:
    """
    This function combine dbt run with dbt test operators and chain them together.
    """
    dbt_run_task = DBTRunOperator(
        task_id=f"dbt_run_{base_task_id}",
        model=model,
    )

    dbt_test_task = DBTRunOperator(
        task_id=f"dbt_test_{base_task_id}",
        model=model,
    )

    dbt_run_task >> dbt_test_task

    return (dbt_run_task, dbt_test_task)
