import boto3

DDL_DATABASE_TEMPLATE = "CREATE DATABASE {database}"
DDL_TABLE_TEMPLATE = """CREATE EXTERNAL TABLE {database}.{table} (
{column_defs}
)
COMMENT {description}
ROW FORMAT SERDE org.apache.hadoop.hive.serde2.OpenCSVSerde
WITH SERDEPROPERTIES (
   "separatorChar" = "\t"
)
TBLPROPERTIES('serialization.null.format'='\\N')
LOCATION {path}
"""
DDL_COL_TEMPLATE = "{column} {type} COMMENT {description}"

class Athena():
    """docstring for Athena."""
    def __init__(self, results_bucket):
        self.athena = boto3.client('athena')
        self.results_bucket = results_bucket

    def create_athena_database(self, database_name):
        statement = DDL_DATABASE_TEMPLATE.format(database=database_name)
        self.athena.start_query_execution(
            QueryString=statement,
            ResultConfiguration={
                'OutputLocation': self.results_bucket,
                'EncryptionConfiguration': {
                    'EncryptionOption':'SSE-S3'
                }
            }
        )

    def create_athena_table(self, database_name, table, table_path):
        statement = canvasdata_schema_to_tableddl(database_name, table, table_path)
        self.athena.start_query_execution(
            QueryString=statement,
            ResultConfiguration={
                'OutputLocation': self.results_bucket,
                'EncryptionConfiguration': {
                    'EncryptionOption':'SSE-S3'
                }
            }
        )

def canvasdata_schema_to_tableddl(database_name, table, table_path):
    table_name = table['tableName']
    columns = list(map(canvascol_to_colddl, table['columns']))
    column_defs = '\n'.join(columns)

    return DDL_COL_TEMPLATE.format(
        database=database_name,
        table=table_name,
        column_defs=column_defs,
        description=table['description'],
        path=table_path
    )

def canvascol_to_colddl(col):
    col_type = col['type'] \
        if 'varchar' not in col['type'] and 'text' not in col['type'] \
        else 'string'
    col_name = col['name']
    description = col['description']

    return DDL_COL_TEMPLATE.format(
        column=col_name,
        type=col_type,
        description=description
    )
