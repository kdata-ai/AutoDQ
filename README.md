Code that has yet to be copied:

- 01_setup_environment.py: pip installs and %magic lines
- 05_Schema Inference.py: main LLM invocation + save logic (only the output schema stub was copied)
- GX_Rule_Metadata(Rule Details).py
- Gold_layer_smart_rule_assistant.py
- schema and table creation.py

### What changed (GX setup inside rule_authoring)

- dq/gx_service.py
  - Initializes a GX Data Context (get_context).
  - Ensures/creates a Spark schema via CREATE SCHEMA IF NOT EXISTS.
  - Creates or updates a Spark Datasource (sources.add_or_update_spark).
  - Creates a table-backed Data Asset (add_table_asset; Delta fallback provided).
  - Exposes helpers: ensure_schema, create_asset, get_context_obj.

- app/cli.py
  - Databricks UI entrypoint: reads widgets (asset_name, schema_name, table_name, datasource_name) and creates the Data Asset.

- pipelines/create_asset.py
  - Small pipeline variant that performs the same asset creation using GXService.

- dq/__init__.py and app/__init__.py
  - Package initializers to enable clean imports.

### How to use (Databricks UI)

1) Open and run one of:
   - Rule_Authoring/app/cli.py
   - Rule_Authoring/pipelines/create_asset.py

2) Set Widgets (in the Databricks UI):
   - asset_name: Name for the GE Data Asset
   - schema_name: Spark schema/database name (will be created if missing)
   - table_name: Existing Spark table name within the schema
   - datasource_name: Optional; defaults to "spark_datasource"

3) Execute the notebook/script. It will:
   - Create the schema if it does not exist
   - Initialize/load the GX Data Context
   - Ensure a Spark Datasource
   - Register the specified table as a GX Data Asset
