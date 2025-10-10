from dq.gx_service import GXService


def run():
    """Pipeline to create a GX data asset using Databricks widgets."""
    try:
        dbutils.widgets.text("asset_name", "")
        dbutils.widgets.text("schema_name", "")
        dbutils.widgets.text("table_name", "")
        dbutils.widgets.text("datasource_name", "spark_datasource")
    except NameError:
        raise RuntimeError("This pipeline expects to run on Databricks with dbutils.widgets available.")

    asset_name = dbutils.widgets.get("asset_name").strip()
    schema_name = dbutils.widgets.get("schema_name").strip()
    table_name = dbutils.widgets.get("table_name").strip()
    datasource_name = dbutils.widgets.get("datasource_name").strip() or "spark_datasource"

    if not asset_name or not schema_name or not table_name:
        raise ValueError("asset_name, schema_name, and table_name are required.")

    service = GXService(datasource_name=datasource_name)
    service.ensure_schema(schema_name)
    service.create_table_asset(asset_name=asset_name, schema_name=schema_name, table_name=table_name)

    print(
        f"Created/updated GX data asset '{asset_name}' for '{schema_name}.{table_name}' in datasource '{datasource_name}'."
    )


if __name__ == "__main__":
    run()


