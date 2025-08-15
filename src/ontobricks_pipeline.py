import subprocess
from functools import reduce

subprocess.check_call(["pip", "install", "git+https://github.com/aktungmak/spark-r2r.git"])
from r2r import Mapping
from databricks.sdk import WorkspaceClient
from pyspark.sql import Window
from pyspark.sql.functions import col, when, explode, row_number
import dlt
import iri

OUTPUT_TABLE = spark.conf.get("output_table")
USERS_TABLE = "users_table"

client = WorkspaceClient()


def scd2_latest(table_name, sequence, *key_cols):
    row_num = "row_num"
    window_spec = Window.partitionBy(*key_cols).orderBy(col(sequence).desc())
    return spark.table(table_name).withColumn(row_num, row_number().over(window_spec)).filter(col(row_num) == 1)


@dlt.table(name=USERS_TABLE)
def fetch_users():
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
    return spark.createDataFrame(users)


mappings = {
    "catalogs":
        Mapping(
            source="system.information_schema.catalogs",
            subject_map=iri.catalog("catalog_name"),
            rdf_type=iri.type("Catalog"),
            predicate_object_maps={
                iri.pred("catalogName"): col("catalog_name"),
                iri.pred("catalogOwnerEmail"): col("catalog_owner"),
            },
        ),
    "schemata":
        Mapping(
            source="system.information_schema.schemata",
            subject_map=iri.schema("catalog_name", "schema_name"),
            rdf_type=iri.type("Schema"),
            predicate_object_maps={
                iri.pred("schemaName"): col("schema_name"),
                iri.pred("schemaOwnerEmail"): col("schema_owner"),
                iri.pred("inCatalog"): iri.catalog("catalog_name"),
            },
        ),
    "tables":
        Mapping(
            source="system.information_schema.tables",
            subject_map=iri.table("table_catalog", "table_schema", "table_name"),
            rdf_type=iri.type("Table"),
            predicate_object_maps={
                iri.pred("tableName"): col("table_name"),
                iri.pred("tableType"): col("table_type"),
                iri.pred("tableOwnerEmail"): col("table_owner"),
                iri.pred("tableInSchema"): iri.schema("table_catalog", "table_schema"),
            },
        ),
    # TODO this is currently very slow so we skip it
    # "columns":
    # Mapping(
    #     source="system.information_schema.columns",
    #     subject_map=iri.column(
    #         "table_catalog", "table_schema", "table_name", "column_name"
    #     ),
    #     rdf_type=iri.type("Column"),
    #     predicate_object_maps={
    #         iri.pred("columnName"): col("column_name"),
    #         iri.pred("columnDataType"): col("data_type"),
    #         iri.pred("inTable"): iri.table("table_catalog", "table_schema", "table_name"),
    #     },
    # ),
    "volumes":
        Mapping(
            source="system.information_schema.volumes",
            subject_map=iri.volume("volume_catalog", "volume_schema", "volume_name"),
            rdf_type=iri.type("Volume"),
            predicate_object_maps={
                iri.pred("volumeName"): col("volume_name"),
                iri.pred("volumeOwnerEmail"): col("volume_owner"),
                iri.pred("volumeInSchema"): iri.schema("volume_catalog", "volume_schema"),
            },
        ),
    "query_history":
        Mapping(
            source="system.query.history",
            subject_map=iri.query("workspace_id", "statement_id"),
            rdf_type=iri.type("Query"),
            predicate_object_maps={
                iri.pred("statementId"): col("statement_id"),
                iri.pred("executedBy"): iri.user("executed_by_user_id"),
                iri.pred("executedInWorkspace"): iri.workspace("workspace_id"),
                iri.pred("queryCompute"): when(
                    col("compute.warehouse_id").isNotNull(),
                    iri.warehouse("workspace_id", "compute.warehouse_id"),
                ).when(
                    col("compute.cluster_id").isNotNull(),
                    iri.cluster("workspace_id", "compute.cluster_id"),
                ), }
        ),
    "warehouses":
        Mapping(
            source="system.compute.warehouses",
            subject_map=iri.warehouse("workspace_id", "warehouse_id"),
            rdf_type=iri.type("Warehouse"),
            predicate_object_maps={
                iri.pred("warehouseId"): col("warehouse_id"),
                iri.pred("warehouseName"): col("warehouse_name"),
                iri.pred("warehouseInWorkspace"): iri.workspace("workspace_id"),
            },
        ),
    "clusters":
        Mapping(
            source="system.compute.clusters",
            subject_map=iri.cluster("workspace_id", "cluster_id"),
            rdf_type=iri.type("Cluster"),
            predicate_object_maps={
                iri.pred("clusterId"): col("cluster_id"),
                iri.pred("clusterName"): col("cluster_name"),
                iri.pred("dbrVersion"): col("dbr_version"),
                iri.pred("clusterOwnerEmail"): col("owned_by"),
                iri.pred("clusterInWorkspace"): iri.workspace("workspace_id"),
            },
        ),
    "users":
        Mapping(
            source=USERS_TABLE,
            subject_map=iri.user("id"),
            rdf_type=iri.type("User"),
            predicate_object_maps={
                iri.pred("userId"): col("id"),
                iri.pred("userEmail"): explode("emails"),
            }),
    "pipelines":
        Mapping(
            source=scd2_latest("system.lakeflow.pipelines", "change_time", "workspace_id", "pipeline_id"),
            subject_map=iri.pipeline("workspace_id", "pipeline_id"),
            rdf_type=iri.type("Pipeline"),
            predicate_object_maps={
                iri.pred("pipelineId"): col("pipeline_id"),
                iri.pred("pipelineName"): col("name"),
                iri.pred("pipelineCreatorEmail"): col("created_by"),
                iri.pred("pipelineRunAsEmail"): col("run_as"),
                iri.pred("pipelineInWorkspace"): iri.workspace("workspace_id"),
            },
        ),
    "notebook_to_table":
        Mapping(
            source=spark.table("system.access.table_lineage").filter(col("entity_metadata.notebook_id").isNotNull()),
            subject_map=iri.notebook("workspace_id", "entity_metadata.notebook_id"),
            predicate_object_maps={
                iri.pred("notebookReadTable"): when(col("source_table_name").isNotNull(),
                                                    iri.table("source_table_catalog", "source_table_schema",
                                                              "source_table_name")),
                iri.pred("notebookWriteTable"): when(col("target_table_name").isNotNull(),
                                                     iri.table("target_table_catalog", "target_table_schema",
                                                               "target_table_name")),
                # TODO what to do with the timestamps?
                # iri.pred("timestamp"): col("event_time"),
            },
        ),
    "query_to_table":
        Mapping(
            source=spark.table("system.access.table_lineage").filter(col("entity_metadata.sql_query_id").isNotNull()),
            subject_map=iri.query("workspace_id", "statement_id"),
            predicate_object_maps={
                iri.pred("queryReadTable"): when(col("source_table_name").isNotNull(),
                                                 iri.table("source_table_catalog", "source_table_schema",
                                                           "source_table_name")),
                iri.pred("queryWriteTable"): when(col("target_table_name").isNotNull(),
                                                  iri.table("target_table_catalog", "target_table_schema",
                                                            "target_table_name")),
                # TODO what to do with the timestamps?
                # iri.pred("timestamp"): col("event_time"),
            },
        ),
    "pipeline_to_table":
        Mapping(
            source=spark.table("system.access.table_lineage").filter(
                col("entity_metadata.dlt_pipeline_info").isNotNull()),
            subject_map=iri.pipeline("workspace_id", "entity_metadata.dlt_pipeline_info.dlt_pipeline_id"),
            predicate_object_maps={
                iri.pred("pipelineReadTable"): when(col("source_table_name").isNotNull(),
                                                    iri.table("source_table_catalog", "source_table_schema",
                                                              "source_table_name")),
                iri.pred("pipelineWriteTable"): when(col("target_table_name").isNotNull(),
                                                     iri.table("target_table_catalog", "target_table_schema",
                                                               "target_table_name")),
                # TODO what to do with the timestamps?
                # iri.pred("timestamp"): col("event_time"),
            },
        ),
    "workspaces":
        Mapping(
            source="system.access.workspaces_latest",
            subject_map=iri.workspace("workspace_id"),
            rdf_type=iri.type("Workspace"),
            predicate_object_maps={
                iri.pred("workspaceName"): col("workspace_name"),
                iri.pred("workspaceStatus"): col("status"),
            }
        )
}

mapped_names = [mapping.to_dlt(spark, name) for name, mapping in mappings.items()]


@dlt.table(name=OUTPUT_TABLE, comment="Databricks metadata in triple format")
def union_all_tables():
    mapped_tables = (
        dlt.read(mapped_name)
        for mapped_name in mapped_names
    )
    return reduce(lambda df1, df2: df1.union(df2), mapped_tables)
