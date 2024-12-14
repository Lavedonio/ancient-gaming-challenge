from pathlib import Path

DBT_PATH = Path(__file__).resolve().parents[1] / 'dbt'
print(DBT_PATH)
