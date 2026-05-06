"""
Snowflake Connection Manager
============================
Thread-safe Snowflake connection helper with query execution,
bulk loading, and SQL file runner utilities.
"""

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from common.config import PipelineConfig

logger = logging.getLogger(__name__)
CONFIG = PipelineConfig()


class SnowflakeManager:
    """
    Manages Snowflake connections and query execution.
    Supports direct queries, parameterized queries, SQL file execution,
    and DataFrame-based results.
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or CONFIG
        self._connection = None

    @contextmanager
    def get_connection(self):
        """Context manager for Snowflake connections with auto-cleanup."""
        import snowflake.connector

        conn = None
        try:
            conn = snowflake.connector.connect(
                account=self.config.SNOWFLAKE_ACCOUNT,
                user=self.config.SNOWFLAKE_USER,
                password=self.config.SNOWFLAKE_PASSWORD,
                database=self.config.SNOWFLAKE_DATABASE,
                warehouse=self.config.SNOWFLAKE_WAREHOUSE,
                role=self.config.SNOWFLAKE_ROLE,
            )
            logger.info("Snowflake connection established")
            yield conn
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Snowflake connection closed")

    def execute_query(
        self, query: str, params: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a dict.

        Args:
            query: SQL query string.
            params: Optional query parameters.

        Returns:
            Dict with query results or metadata.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Try to fetch results
                if cursor.description:
                    columns = [col[0].lower() for col in cursor.description]
                    row = cursor.fetchone()
                    if row:
                        return dict(zip(columns, row))

                # For DML statements, return affected rows
                return {
                    "rows_affected": cursor.rowcount,
                    "rows_loaded": cursor.rowcount,
                    "status": "success",
                }
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                logger.error(f"Query: {query[:200]}...")
                raise
            finally:
                cursor.close()

    def execute_query_to_df(
        self, query: str, params: Optional[Dict] = None
    ) -> Optional[pd.DataFrame]:
        """Execute a query and return results as a pandas DataFrame."""
        with self.get_connection() as conn:
            try:
                if params:
                    df = pd.read_sql(query, conn, params=params)
                else:
                    df = pd.read_sql(query, conn)
                logger.info(f"Query returned {len(df)} rows")
                return df
            except Exception as e:
                logger.error(f"DataFrame query failed: {e}")
                raise

    def run_sql_file(
        self, file_path: str, params: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Read and execute a SQL file.

        Supports multi-statement SQL files separated by semicolons.
        Returns metadata from the last statement executed.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        sql_content = path.read_text(encoding="utf-8")
        logger.info(f"Executing SQL file: {path.name}")

        # Split on semicolons but ignore empty statements
        statements = [
            s.strip() for s in sql_content.split(";")
            if s.strip() and not s.strip().startswith("--")
        ]

        last_result = None
        total_rows = 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for i, stmt in enumerate(statements):
                    logger.debug(f"Executing statement {i + 1}/{len(statements)}")
                    if params:
                        cursor.execute(stmt, params)
                    else:
                        cursor.execute(stmt)
                    total_rows += cursor.rowcount or 0

                last_result = {
                    "rows_affected": total_rows,
                    "statements_executed": len(statements),
                    "status": "success",
                    "file": path.name,
                }
            except Exception as e:
                logger.error(f"SQL file execution failed at statement {i + 1}: {e}")
                raise
            finally:
                cursor.close()

        return last_result

    def bulk_load(
        self,
        table: str,
        df: pd.DataFrame,
        schema: Optional[str] = None,
        if_exists: str = "append",
    ) -> Dict[str, Any]:
        """
        Bulk load a DataFrame into a Snowflake table using write_pandas.

        Args:
            table: Target table name.
            df: DataFrame to load.
            schema: Target schema (defaults to config).
            if_exists: 'append' or 'replace'.

        Returns:
            Dict with load statistics.
        """
        from snowflake.connector.pandas_tools import write_pandas

        schema = schema or self.config.STAGING_SCHEMA

        with self.get_connection() as conn:
            try:
                success, num_chunks, num_rows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=table.upper(),
                    schema=schema.upper(),
                    database=self.config.SNOWFLAKE_DATABASE.upper(),
                    overwrite=(if_exists == "replace"),
                )

                result = {
                    "success": success,
                    "chunks": num_chunks,
                    "rows_loaded": num_rows,
                    "table": f"{schema}.{table}",
                }
                logger.info(f"Bulk load: {num_rows} rows → {schema}.{table}")
                return result
            except Exception as e:
                logger.error(f"Bulk load failed: {e}")
                raise

    def test_connection(self) -> bool:
        """Test Snowflake connectivity. Returns True if successful."""
        try:
            result = self.execute_query("SELECT CURRENT_TIMESTAMP() AS ts")
            logger.info(f"Connection test passed: {result}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
