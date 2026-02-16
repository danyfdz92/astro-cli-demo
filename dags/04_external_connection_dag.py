"""
Connecting to External Systems
a. Leverage at least one shared connection in your DAGs. It’s common to include a connection to a cloud service provider or a cloud database like Snowflake or Databricks.
b. BONUS: Demonstrate a realistic use case with your DAG.

Latinas In Tech - JobStreet Job Postings - Malaysia

This DAG contains 4 tasks:

- Extract: azrai99/job-dataset from Hugging Face (https://huggingface.co/datasets/azrai99/job-dataset).
  - This dataset brings together a comprehensive collection of job listings sourced from JobStreet. It will be used to analyze job market trends, uncover hiring patterns, and better understand the dynamics shaping employment opportunities.

- Validate: Confirm that the records have been successfully extracted and that the dataset includes all columns required by the production.careers.job_listings table.

- Transform: Parse salary range, compute median, convert MYR to USD. 
  - Since the dataset features job listings from Malaysia, salaries are originally reported in Malaysian Ringgit (MYR). To enable clearer comparison and interpretation, we will convert MYR to USD, providing a more standardized view of compensation levels.

- Load: Write to Snowflake table production.careers.job_listings (connection_id: data_warehouse).

Schedule - @daily

Start_date - December 1, 2021

Catchup - False
  - The scheduler will only run the most recent interval
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

CONN_ID = "data_warehouse"
DATASET_NAME = "azrai99/job-dataset"

# Snowflake target:
DATABASE_NAME = "PRODUCTION"
SCHEMA_NAME = "CAREERS"
TABLE_NAME = "JOB_LISTINGS"  # plural — must match actual table name in Snowflake
SNOWFLAKE_ROLE = "ASTRONOMER_RW"

# MYR to USD rate; 
DEFAULT_MYR_TO_USD = 0.22
# Number of rows to read
DEFAULT_MAX_ROWS = 100000


def _parse_salary_median_myr(salary_raw: Any) -> Optional[float]:
    """
    Parse salary string (e.g. "RM 2,800 – RM 3,200 per month") and return median in MYR.
    Returns None if missing or unparseable.
    """
    if salary_raw is None or (isinstance(salary_raw, str) and salary_raw.strip().lower() in ("null", "")):
        return None
    s = str(salary_raw).strip()
    # Match numbers after "RM" (with optional commas)
    numbers = re.findall(r"RM\s*([\d,]+)", s, re.IGNORECASE)
    if not numbers:
        return None
    values = [float(n.replace(",", "")) for n in numbers]
    if len(values) == 1:
        return values[0]
    return (min(values) + max(values)) / 2.0


@dag(
    dag_id="external_connection_etl",
    schedule="@daily",
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=["external", "connection", "etl"],
)
def external_connection_etl():
    @task
    def extract() -> List[Dict[str, Any]]:
        """Load job listings from Hugging Face dataset azrai99/job-dataset."""
        from datasets import load_dataset

        try:
            max_rows = int(Variable.get("job_etl_max_rows", default_var=DEFAULT_MAX_ROWS))
        except (ValueError, TypeError):
            max_rows = DEFAULT_MAX_ROWS
        logger.info("Loading dataset %s (max_rows=%s)", DATASET_NAME, max_rows)
        # Load only the first max_rows rows from the dataset
        ds = load_dataset(DATASET_NAME, split=f"train[:{max_rows}]")
        rows = [dict(r) for r in ds]
        logger.info("Extracted %d rows", len(rows))
        return rows

    @task
    def validate(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Basic validation: require non-empty and expected keys."""
        if not records:
            raise ValueError("No records extracted")
        # Dataset has job_id, job_title, company, descriptions, location, category, subcategory, role, type, salary, listingDate
        sample = records[0]
        if "job_id" not in sample and "job_title" not in sample:
            raise ValueError("Records missing expected fields (job_id / job_title)")
        return records

    @task
    def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform: compute salary median from range and convert MYR to USD.
        Adds key salary_median_usd (None when salary is missing).
        """
        try:
            rate = float(Variable.get("myr_to_usd_rate", default_var=DEFAULT_MYR_TO_USD))
        except Exception:
            rate = DEFAULT_MYR_TO_USD
        logger.info("Using MYR to USD rate: %s", rate)

        out = []
        for r in records:
            row = dict(r)
            salary_raw = row.get("salary")
            median_myr = _parse_salary_median_myr(salary_raw)
            row["salary_median_usd"] = round(median_myr * rate, 2) if median_myr is not None else None
            out.append(row)
        return out

    @task
    def load(records: List[Dict[str, Any]]) -> int:
        """
        Load records into Snowflake PRODUCTION.CAREERS.JOB_LISTINGS.
        Uses connection data_warehouse; unquoted identifiers for Snowflake uppercase resolution.
        """
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas

        if not records:
            logger.warning("No records to load")
            return 0

        hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        connection = BaseHook.get_connection(CONN_ID)

        cursor.execute(f'USE ROLE "{SNOWFLAKE_ROLE.upper()}"')

        db = DATABASE_NAME.upper()
        schema = SCHEMA_NAME.upper()
        table = TABLE_NAME.upper()
        logger.info("Database: %s, Schema: %s, Table: %s", db, schema, table)

        # Unquoted identifiers so Snowflake resolves to uppercase (avoids "careers" vs CAREERS)
        truncate_sql = f"TRUNCATE TABLE IF EXISTS {db}.{schema}.{table}"
        logger.info("Executing: %s", truncate_sql)
        cursor.execute(truncate_sql)

        logger.info(records)
        df = pd.DataFrame(records)
        df.rename(columns={"listingDate": "LISTING_DATE", "role": "JOB_ROLE"}, inplace=True)
        # "role" is a Snowflake reserved keyword; use job_role to avoid invalid identifier errors
        if "job_id" in df.columns:
            df["job_id"] = pd.to_numeric(df["job_id"], errors="coerce")
        if "salary_median_usd" in df.columns:
            df["salary_median_usd"] = pd.to_numeric(df["salary_median_usd"], errors="coerce")
        df.columns = [x.upper() for x in df.columns]
        logger.info(df.head())
        success, _, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table,
            schema=schema,
            database=db,
            auto_create_table=False,
        )
        if not success:
            raise RuntimeError("write_pandas did not succeed")
        logger.info("Loaded %d rows into %s.%s.%s", num_rows, db, schema, table)
        return int(num_rows)

    load(transform(validate(extract())))


external_connection_etl()
