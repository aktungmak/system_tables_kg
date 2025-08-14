from pyspark.sql.functions import format_string, lit, url_encode
from pyspark.sql.column import Column

ontology_root = "dbx:"

def _encode_all(*args) -> list:
    return [url_encode(arg) for arg in args]

# TODO add metastore ID
def catalog(catalog_name: str) -> Column:
    args = _encode_all(catalog_name)
    return format_string(ontology_root + "catalog/%s", *args)


def schema(catalog_name: str, schema_name: str) -> Column:
    args = _encode_all(catalog_name, schema_name)
    return format_string(ontology_root + "schema/%s/%s", *args)


def table(catalog_name: str, schema_name: str, table_name: str) -> Column:
    args = _encode_all(catalog_name, schema_name, table_name)
    return format_string(ontology_root + "table/%s/%s/%s", *args)


def column(
    catalog_name: str, schema_name: str, table_name: str, column_name: str
) -> Column:
    args = _encode_all(catalog_name, schema_name, table_name, column_name)
    return format_string(ontology_root + "column/%s/%s/%s/%s", *args)

def volume(catalog_name: str, schema_name: str, volume_name: str) -> Column:
    args = _encode_all(catalog_name, schema_name, volume_name)
    return format_string(ontology_root + "volume/%s/%s/%s", *args)


def query(workspace_id: str, statement_id: str) -> Column:
    return format_string(
        ontology_root + "query/%s/%s", workspace_id, statement_id
    )

def user(user_id: str) -> Column:
    return format_string(ontology_root + "user/%s", user_id)

def warehouse(workspace_id: str, warehouse_id: str) -> Column:
    return format_string(
        ontology_root + "warehouse/%s/%s", workspace_id, warehouse_id
    )

def cluster(workspace_id: str, cluster_id: str) -> Column:
    return format_string(ontology_root + "cluster/%s/%s", workspace_id, cluster_id)

def job(workspace_id: str, job_id: str) -> Column:
    return format_string(ontology_root + "job/%s/%s", workspace_id, job_id)

def task(workspace_id: str, job_id: str, task_key: str) -> Column:
    return format_string(ontology_root + "task/%s/%s/%s", workspace_id, job_id, task_key)

def pipeline(workspace_id: str, pipeline_id: str) -> Column:
    return format_string(ontology_root + "pipeline/%s/%s", workspace_id, pipeline_id)

def workspace(workspace_id: str) -> Column:
    return format_string(ontology_root + "workspace/%s", workspace_id)

def pred(name: str) -> str:
    return ontology_root + "pred/" + name

def type(name: str) -> str:
    return ontology_root + "type/"+ name