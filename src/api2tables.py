import sys
import subprocess

subprocess.check_call(["pip", "install", "sql_metadata"])

from pyspark.sql import SparkSession, DataFrame

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogType
from sql_metadata import Parser

client = WorkspaceClient()
workspace_id = client.get_workspace_id()

users = (
    {
        "id": user.id,
        "active": user.active,
        "display_name": user.display_name,
        "groups": [group.value for group in user.groups],
        "emails": [email.value for email in user.emails],
    }
    for user in client.users.list()
)

pipelines = (
    {
        "id": p.pipeline_id,
        "name": p.name,
        "creator_user_name": p.creator_user_name,
        "run_as_user_name": p.run_as_user_name,
        "workspace_id": workspace_id,
    }
    for p in client.pipelines.list_pipelines()
)

columns = (
    col.as_dict()
    for c in client.catalogs.list()
    for s in client.schemas.list(c.name)
    for t in client.tables.list(c.name, s.name)
    for col in t.columns
)


def query_to_table():
    for q in client.query_history.list():
        try:
            for table_name in Parser(q.query_text, disable_logging=True).tables:
                table = client.tables.get(table_name)
                yield {
                    "workspace_id": workspace_id,
                    "query_id": q.query_id,
                    "metastore_id": table.metastore_id,
                    "catalog_name": table.catalog_name,
                    "schema_name": table.schema_name,
                    "table_name": table.name,
                }
        except Exception as e:
            print(e)
            continue


table_to_pipeline = (
    {
        "metastore_id": t.metastore_id,
        "catalog_name": t.catalog_name,
        "schema_name": t.schema_name,
        "table_name": t.name,
        "pipeline_id": t.pipeline_id,
        "workspace_id": workspace_id,
    }
    for c in client.catalogs.list()
    for s in client.schemas.list(c.name)
    for t in client.tables.list(c.name, s.name)
    if c.catalog_type != CatalogType.DELTASHARING_CATALOG
       and t.pipeline_id
)


def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


options = {
    "users": users,
    "pipelines": pipelines,
    "query_to_table": query_to_table(),
    "table_to_pipeline": table_to_pipeline,
    "columns": columns
}


def main():
    output_catalog = sys.argv[1]
    output_schema = sys.argv[2]
    selection = sys.argv[3]
    schema = f"{output_catalog}.{output_schema}"
    print(f"writing selected API {selection} to {schema}")
    spark = get_spark()
    iterator = options[selection]
    spark.createDataFrame(iterator).write.mode("overwrite").saveAsTable(f"{schema}.{selection}")


if __name__ == "__main__":
    main()
