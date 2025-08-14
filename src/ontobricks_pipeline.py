import subprocess
from functools import reduce

from databricks.sdk import WorkspaceClient

subprocess.check_call(["pip", "install", "git+https://github.com/aktungmak/spark-r2r.git"])
from r2r import Mapping
from pyspark.sql.functions import col, when, explode
import dlt
import iri

OUTPUT_TABLE = spark.conf.get("output_table")
USERS_TABLE = "users_table"

client = WorkspaceClient()


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


mappings = [
    Mapping(
        source="system.information_schema.catalogs",
        subject_map=iri.catalog("catalog_name"),
        rdf_type=iri.type("catalog"),
        predicate_object_maps={
            iri.pred("catalog_name"): col("catalog_name"),
            iri.pred("catalog_owner_email"): col("catalog_owner"),
        },
    ),
    Mapping(
        source="system.information_schema.schemata",
        subject_map=iri.schema("catalog_name", "schema_name"),
        rdf_type=iri.type("schema"),
        predicate_object_maps={
            iri.pred("schema_name"): col("schema_name"),
            iri.pred("schema_owner_email"): col("schema_owner"),
            iri.pred("in_catalog"): iri.catalog("catalog_name"),
        },
    ),
    Mapping(
        source="system.information_schema.tables",
        subject_map=iri.table("table_catalog", "table_schema", "table_name"),
        rdf_type=iri.type("table"),
        predicate_object_maps={
            iri.pred("table_name"): col("table_name"),
            iri.pred("table_type"): col("table_type"),
            iri.pred("table_owner_email"): col("table_owner"),
            iri.pred("in_schema"): iri.schema("table_catalog", "table_schema"),
        },
    ),
    # TODO this is currently very slow so we skip it
    # Mapping(
    #     source="system.information_schema.columns",
    #     subject_map=iri.column(
    #         "table_catalog", "table_schema", "table_name", "column_name"
    #     ),
    #     rdf_type=iri.type("column"),
    #     predicate_object_maps={
    #         iri.pred("column_name"): col("column_name"),
    #         iri.pred("column_data_type"): col("data_type"),
    #         iri.pred("in_table"): iri.table("table_catalog", "table_schema", "table_name"),
    #     },
    # ),
    Mapping(
        source="system.information_schema.volumes",
        subject_map=iri.volume("volume_catalog", "volume_schema", "volume_name"),
        rdf_type=iri.type("volume"),
        predicate_object_maps={
            iri.pred("volume_name"): col("volume_name"),
            iri.pred("volume_owner_email"): col("volume_owner"),
            iri.pred("in_schema"): iri.schema("volume_catalog", "volume_schema"),
        },
    ),
    Mapping(
        source="system.query.history",
        subject_map=iri.query("workspace_id", "statement_id"),
        rdf_type=iri.type("query"),
        predicate_object_maps={
            iri.pred("statement_id"): col("statement_id"),
            iri.pred("executed_by"): iri.user("executed_by_user_id"),
            iri.pred("workspace_id"): iri.workspace("workspace_id"),
            iri.pred("query_compute"): when(
                col("compute.warehouse_id").isNotNull(),
                iri.warehouse("workspace_id", "compute.warehouse_id"),
            ).when(
                col("compute.cluster_id").isNotNull(),
                iri.cluster("workspace_id", "compute.cluster_id"),
            ), }
    ),
    Mapping(
        source="system.compute.warehouses",
        subject_map=iri.warehouse("workspace_id", "warehouse_id"),
        rdf_type=iri.type("warehouse"),
        predicate_object_maps={
            iri.pred("warehouse_id"): col("warehouse_id"),
            iri.pred("warehouse_name"): col("warehouse_name"),
            iri.pred("workspace_id"): iri.workspace("workspace_id"),
        },
    ),
    Mapping(
        source="system.compute.clusters",
        subject_map=iri.cluster("workspace_id", "cluster_id"),
        rdf_type=iri.type("cluster"),
        predicate_object_maps={
            iri.pred("cluster_id"): col("cluster_id"),
            iri.pred("cluster_name"): col("cluster_name"),
            iri.pred("dbr_version"): col("dbr_version"),
            iri.pred("cluster_owner_email"): col("owned_by"),
            iri.pred("workspace_id"): iri.workspace("workspace_id"),
        },
    ),
    Mapping(
        source=USERS_TABLE,
        subject_map=iri.user("id"),
        rdf_type=iri.type("user"),
        predicate_object_maps={
            iri.pred("user_id"): col("id"),
            iri.pred("user_email"): explode("emails"),
        }),
    Mapping(
        source="system.lakeflow.pipelines",
        subject_map=iri.pipeline("workspace_id", "pipeline_id"),
        rdf_type=iri.type("pipeline"),
        predicate_object_maps={
            iri.pred("pipeline_id"): col("pipeline_id"),
            iri.pred("pipeline_name"): col("name"),
            iri.pred("creator_email"): col("created_by"),
            iri.pred("run_as_email"): col("run_as"),
            iri.pred("workspace_id"): iri.workspace("workspace_id"),
        },
    ),
    Mapping(
        source="system.access.table_lineage",
        subject_map=when(col("entity_metadata.notebook_id").isNotNull(),
                         iri.notebook("workspace_id", "entity_metadata.notebook_id")) \
            .when(col("entity_metadata.sql_query_id").isNotNull(),
                  iri.query("workspace_id", "statement_id")) \
            .when(col("entity_metadata.dlt_pipeline_info").isNotNull(),
                  iri.pipeline("workspace_id", "entity_metadata.dlt_pipeline_info.dlt_pipeline_id")),
        predicate_object_maps={
            iri.pred("read"): when(col("source_table_name").isNotNull(),
                                   iri.table("source_table_catalog", "source_table_schema", "source_table_name")),
            iri.pred("wrote"): when(col("target_table_name").isNotNull(),
                                    iri.table("target_table_catalog", "target_table_schema", "target_table_name")),
            iri.pred("timestamp"): col("event_time"),
        },

    ),
    Mapping(
        source="system.access.workspaces_latest",
        subject_map=iri.workspace("workspace_id"),
        predicate_object_maps={
            iri.pred("workspace_name"): col("workspace_name"),
            iri.pred("workspace_status"): col("status"),
        }
    )
]

mapped_names = [mapping.to_dlt(spark, mapping.source.replace('.', '_')+'_mapped') for mapping in mappings]


@dlt.table(name=OUTPUT_TABLE, comment="Databricks metadata in triple format")
def union_all_tables():
    mapped_tables = (
        dlt.read(mapped_name)
        for mapped_name in mapped_names
    )
    return reduce(lambda df1, df2: df1.union(df2), mapped_tables)
