import os
import shutil

from datetime import datetime
import pandas as pd
import re

from dagster import asset, AssetIn
import json

from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource

from io import BytesIO

from trino.exceptions import TrinoUserError


@asset(group_name="Data_Integration_excel",
       required_resource_keys={"trino"})
def obtain_data_from_excels(context):
    """
    Parses the different files found in input_files, generates the minimum info for init to work
    """
    trino = context.resources.trino
    minio_cli = init_minio_server(9000, "minio", "minio123")
    i = 0
    tables = dict()
    try:
        buckets = minio_cli.list_buckets()
        if len(buckets) > 0:
            with (trino.get_connection() as conn):
                if len(check_tables_in_schema(conn, "my_catalog.info")) == 0:
                    create_info_table(conn)
                for bucket in buckets:
                    if 'processed' not in bucket.name and 'json' not in bucket.name:
                        fixed_bucket_name = fix_string(bucket.name)
                        context.log.info(f"Processing files in bucket: {fixed_bucket_name}")
                        if len(check_tables_in_schema(conn, f"my_catalog.{fixed_bucket_name}")) == 0:
                            query = f'''create schema if not exists my_catalog.{fixed_bucket_name}'''
                            input_query(conn, query)
                        # Obtener la lista de objetos en el bucket
                        objects = minio_cli.list_objects(bucket.name, recursive=True)
                        for obj in objects:
                            if obj.object_name.endswith(".xlsx"):
                                # Leer el contenido del archivo Excel
                                file_data = minio_cli.get_object(bucket.name, obj.object_name)
                                excel_data = file_data.read()
                                # Crear un objeto BytesIO a partir del contenido del archivo
                                file_stream = BytesIO(excel_data)
                                # Leer el archivo Excel y procesar su contenido
                                for sheet in pd.ExcelFile(file_stream).sheet_names:
                                    context.log.info(f"Reading file {obj.object_name} and sheet {str(sheet)}")
                                    lista, rows, columns = read_files_op(context, file_stream,
                                                                         str(sheet))  # Función para leer archivos Excel
                                    if len(lista) != 0:
                                        name = (fix_string(obj.object_name).replace("xlsx", "") + "_" + fix_string(
                                            sheet))[0:60]
                                        it = {
                                            'name': name,
                                            'bucket_name': fixed_bucket_name,
                                            'rows': rows,
                                            'columns': columns
                                        }
                                        # Función para insertar datos en la base de datos
                                        insert_table_to_db(conn, lista, name, fixed_bucket_name)
                                        tables.update({str(i): it})
                                        i += 1
                                # Mover el archivo procesado a una carpeta diferente
                                minio_mv(minio_cli, bucket.name, obj)
                                conn.commit()
    except S3Error as e:
        print("Error:", e)
        return tables
    return tables


@asset(ins={"tables": AssetIn("obtain_data_from_excels")},
       group_name="Data_Integration_excel",
       required_resource_keys={"trino"})
def transform_data(context, tables):
    """
    :param context: the context utilized during the procedure, allows us to obtain resources
    :param tables: the information passed down from the previous asset
    Its main function is to generate the tables using dictionaries obtained from iterate_lib, and saving a series
    of persistence files which we could later use in case of system malfunction
    """
    trino = context.resources.trino
    with trino.get_connection() as conn:
        if len(tables) != 0:
            for ele in tables.keys():
                try:
                    name = tables[ele]['name']
                    bucket = tables[ele]['bucket_name']
                    for row in tables[ele]['rows']:
                        columns = tables[ele]['columns']
                        filtered_row, filtered_columns = reformat_rows(row, columns)
                        query = '''insert into my_catalog.{}.{} ({}) values ({})'''.format(bucket, name,
                                                                                           str(filtered_columns).replace(
                                                                                               "'",
                                                                                               "")[
                                                                                           1:-1],
                                                                                           str(filtered_row).replace(
                                                                                               '"',
                                                                                               "")[
                                                                                           1:-1])
                        input_query(conn, query)
                except Exception as e:
                    context.log.error(f'Error creating schema: {e}')
                    return []
            conn.commit()
    return []


@asset(group_name="Data_Integration_json")
def obtain_data_from_json(context):
    minio_cli = init_minio_server(9000, "minio", "minio123")
    jsons = []
    name = "configuration"
    objects = minio_cli.list_objects(name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".json"):
            file_data = minio_cli.get_object(name, obj.object_name)
            json_data = json.loads(file_data.read())
            jsons.append(json_data)
    return jsons


@asset(ins={"jsons": AssetIn("obtain_data_from_json")},
       group_name="Data_Integration_json",
       required_resource_keys={"trino"})
def transform_json(context, jsons):
    trino = context.resources.trino
    with trino.get_connection() as conn:
        max_len = 5
        try:
            for ele in jsons:
                company_initials = ""
                if isinstance(ele['products'][0]['company']['name'], str):
                    company_name = fix_string(ele['products'][0]['company']['name'])
                    query = '''create schema if not exists my_catalog.{}'''.format(company_name)
                    input_query(conn, query)
                    count = 0
                    for word in str(company_name).split("_"):
                        count += 1
                        if count == max_len:
                            break
                        company_initials += word[0]
                    workflow_steps = ele['workflowSteps']
                    for workflow in workflow_steps:
                        workflow_name = workflow['name']
                        wds_data_names = ''
                        for workflow_step_data_field in workflow['workflowStepDataFields']:
                            name = workflow_step_data_field['name']
                            data_type = workflow_step_data_field['workflowDataType']
                            wds_data_names += name + " " + to_sql(data_type) + ", "
                            table_name = fix_string(company_initials + "_" + workflow_name)
                        if len(wds_data_names) > 0:
                            query = '''create table if not exists my_catalog.{}.{} ({})'''.format(company_name,
                                                                                                  table_name,
                                                                                                  (str(wds_data_names[:-2])))

                            input_query(conn, query)
        except KeyError as e:
            context.log.error(f'Error creating schema: {e}')
    return []


def read_files_op(context, path, sheet):
    try:
        df = pd.read_excel(path, sheet_name=sheet)
        if not df.empty:
            lista = []
            rows = []
            columns = []
            bad_words = [
                "nan", "NULL", ''
            ]
            for row in df.itertuples(index=False):
                rows.append([str(value) for value in row])
            all_columns = df.columns.tolist()
            for column in all_columns:
                column_array = df[column].tolist()
                corrected_column = fix_string(column)
                column_value = None
                for value in column_array:
                    if str(value) not in bad_words:
                        column_value = value
                        break
                if column_value is None:
                    column_value = 'text'
                lista.append([corrected_column, identify_string_type(str(column_value))])
                columns.append(corrected_column)
            return lista, rows, columns
        else:
            return [], [], []
    except Exception as e:
        context.log.error(f"Error al leer el archivo {path}: {str(e)}")
        raise e


def to_sql(string: str):
    study = string.lower()
    if study == "number":
        return "bigint"
    elif study == "date":
        return "date"
    else:
        return "varchar"


def minio_mv(minio_cli, bucket_name, obj):
    minio_cli.copy_object(f"processed-{bucket_name}", obj.object_name, CopySource(bucket_name, obj.object_name))
    minio_cli.remove_object(bucket_name, obj.object_name)


def identify_string_type(input_string):
    # Regular expressions for matching different types
    timestamp_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$'
    # Check if it's a timestamp
    if re.match(timestamp_pattern, input_string) is not None:
        return "Timestamp(0)"
    else:
        return "varchar"


def fix_string(string):
    special_replacements = {
        '%': 'porcentaje_',
        'ñ': 'n',
        'Ñ': 'N'
    }
    special_characters = [
        ';', '--', '/*', '*/', "'", '"', '\\', '%', '_', '<', '>', '=', '+', '-', '*', '/', '@', '#', '!', '~', '`',
        '|'
        , '&', '^', '$', '?', '(', ')', '[', ']', '{', '}', ',', '.', ':', ' '
    ]

    # Create a set for quick lookup
    special_char_set = set(special_characters)

    # Build the final string using a list for better performance
    result = []
    for char in string:
        if char in special_replacements:
            result.append(special_replacements[char])
        elif char in special_char_set:
            result.append('_')
        else:
            result.append(char)

    # Join the list into a string
    final_string = ''.join(result)

    # Replace any double underscores with a single underscore
    while '__' in final_string:
        final_string = final_string.replace('__', '_')

    return final_string


def reformat_rows(row, columns):
    row_copy = row.copy()
    columns_copy = columns.copy()
    count = 0
    for pos, ele in enumerate(row):
        tipo = identify_string_type(str(ele))
        if tipo == "Timestamp(0)" and not "datetime.datetime" in str(ele) and not "datetime.time" in str(ele):
            value = "Timestamp '" + str(ele) + "'"
            row_copy.pop(pos - count)
            row_copy.insert(pos - count, value)
        elif tipo == "Timestamp(0)" and "datetime.datetime" in str(ele):
            value = row_copy.pop(pos - count)
            numbers = str(value).replace("datetime.datetime(", "").replace(")", "").split(", ")
            numbers_copy = numbers.copy()
            for p, e in enumerate(numbers_copy):
                if e == "0":
                    numbers.pop(p)
                    numbers.insert(p, "00")
            while len(numbers) < 6:
                numbers.append("00")
            if len(numbers) > 6:
                row_copy.insert(pos - count,
                                f"TIMESTAMP '{numbers[0]}-{numbers[1]}-{numbers[2]} {numbers[3]}:{numbers[4]}:{numbers[5]}.{numbers[6]}'")
            else:
                row_copy.insert(pos - count,
                                f"TIMESTAMP '{numbers[0]}-{numbers[1]}-{numbers[2]} {numbers[3]}:{numbers[4]}:{numbers[5]}'")

        elif tipo == "Timestamp(0)" and "datetime.time" in str(ele):
            row_copy.pop(pos - count)
            columns_copy.pop(pos - count)
            count += 1
        elif tipo == "varchar" and (str(ele) == "nan" or str(ele) == "NaT"):
            row_copy.pop(pos - count)
            columns_copy.pop(pos - count)
            count += 1

    return row_copy, columns_copy


def input_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return


def check_tables_in_schema(conn, schema):
    try:
        query = f"""SHOW TABLES FROM {schema}"""
        cursor = conn.cursor()
        cursor.execute(query)
        tables = cursor.fetchall()
        conn.commit()
    except TrinoUserError as e:
        return []
    return tables


def check_schemas_in_catalog(conn, catalog):
    try:
        query = f"""SHOW schemas FROM {catalog}"""
        cursor = conn.cursor()
        cursor.execute(query)
        tables = cursor.fetchall()
        conn.commit()
    except TrinoUserError as e:
        return []
    return tables


def open_persisted_queries(conn, path):
    with open(path, "r", encoding="UTF-8") as file:
        f = file.read()
        for query in f.split("\n")[:-1]:
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.fetchall()
            conn.commit()


def insert_table_to_db(conn, lista, name, bucket):
    current_datetime = str(datetime.now()).split(".")[0]
    columns_definition = ', '.join([f'{col[0]} {col[1]}' for col in lista])

    query = '''create table if not exists my_catalog.{}.{} ({})'''.format(bucket, name,
                                                                                   columns_definition)
    input_query(conn, query)
    query = '''insert into my_catalog.info.files (table_name, creation) values ('{}',{})'''.format(
        bucket + "." + name, "timestamp" + " '" + current_datetime + "'")
    input_query(conn, query)


def create_info_table(conn):
    query = '''create schema if not exists my_catalog.info'''
    input_query(conn, query)
    query = '''create table if not exists my_catalog.info.files (table_name varchar,creation TIMESTAMP)'''
    input_query(conn, query)


def init_minio_server(port: int, name: str, password: str):
    client = Minio(
        f"host.docker.internal:{port}",
        access_key=name,
        secret_key=password,
        secure=False
    )
    return client



