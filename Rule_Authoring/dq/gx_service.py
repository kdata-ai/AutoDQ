from typing import List, Optional
from pyspark.sql import SparkSession

try:
    # GE >= 0.18/1.x
    from great_expectations.data_context import get_context  # type: ignore
except Exception:  # pragma: no cover
    # GE legacy fallback
    from great_expectations import get_context  # type: ignore


class GXService:
    """Service for working with Great Expectations and Spark in Databricks.

    Responsibilities:
    - Initialize and cache a GE Data Context
    - Ensure a Spark schema (database) exists
    - Create a Spark-backed Datasource and Table Data Asset
    """

    def __init__(self, datasource_name: str = "spark_datasource") -> None:
        self.datasource_name = datasource_name
        self._context = None

    def get_spark(self) -> SparkSession:
        """Return an active SparkSession, creating one if needed."""
        return SparkSession.builder.getOrCreate()

    def get_context(self):
        """Return a cached GE Data Context instance."""
        if self._context is None:
            self._context = get_context()
        return self._context

    def ensure_schema(self, schema_name: str) -> None:
        """Create a schema (database) in the metastore if it does not exist."""
        if not schema_name or not schema_name.strip():
            raise ValueError("schema_name must be provided")
        spark = self.get_spark()
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")

    def get_or_create_spark_datasource(self):
        """Create or fetch a Spark Datasource in GE."""
        context = self.get_context()
        # New-style API
        try:
            return context.sources.add_or_update_spark(name=self.datasource_name)
        except AttributeError:
            # Fallback for older GE versions is not implemented here intentionally.
            raise RuntimeError(
                "Your installed Great Expectations version does not support 'sources.add_or_update_spark'."
            )

    def create_table_asset(self, asset_name: str, schema_name: str, table_name: str):
        """Create a Spark Table Asset under the configured Datasource.

        Returns the created asset instance.
        """
        if not asset_name or not schema_name or not table_name:
            raise ValueError("asset_name, schema_name, and table_name are required")

        self.ensure_schema(schema_name)
        datasource = self.get_or_create_spark_datasource()

        # Prefer generic table asset; fall back to Delta asset if needed.
        try:
            asset = datasource.add_table_asset(
                name=asset_name, table_name=table_name, schema_name=schema_name
            )
        except Exception:
            asset = datasource.add_delta_table_asset(
                name=asset_name, database=schema_name, table=table_name
            )

        try:
            self.get_context().save()
        except Exception:
            # Some GE versions do not require explicit save
            pass

        return asset

    def list_assets(self) -> List[str]:
        """Return the list of asset names under the configured Datasource."""
        context = self.get_context()
        ds = context.get_datasource(self.datasource_name)
        try:
            return [a.name for a in ds.assets]
        except Exception:
            return []


# Convenience module-level helpers
_default_service = GXService()


def ensure_schema(schema_name: str) -> None:
    _default_service.ensure_schema(schema_name)


def create_asset(asset_name: str, schema_name: str, table_name: str, datasource_name: Optional[str] = None):
    if datasource_name:
        _default_service.datasource_name = datasource_name
    return _default_service.create_table_asset(asset_name, schema_name, table_name)


def get_context_obj():
    return _default_service.get_context()
