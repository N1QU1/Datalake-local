from datetime import datetime
import pandas as pd
import os
import re

from dagster import asset, AssetIn

from unidecode import unidecode
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource

from io import BytesIO

@asset(group_name="Data_Integration_excel",
       required_resource_keys={"postgres"})
def obtain_data_from_excels(context):
    postgres = context.resources.postgres
    minio_cli = init_minio_server(19000, "minio", "minio123")
    i = 0
    tables = dict()
    try:
        with open("config_file.sql", "w") as cf:
            buckets = minio_cli.list_buckets()
            if len(buckets) > 0:
                with (postgres.get_connection() as conn):
                    if not check_if_schema_exists(conn, "info")[0]:
                        create_info_table(conn, cf)
                    for bucket in buckets:
                        context.log.info(check_if_schema_exists(conn, f"{bucket.name}")[0])
                        if "configuration" not in bucket.name:
                            add_directory_to_config(bucket.name, minio_cli)
                            if not (check_if_schema_exists(conn, f"{bucket.name}")[0]):
                                #query = f'''create schema if not exists my_catalog.{bucket.name}'''
                                query = f'''create schema if not exists {bucket.name};'''
                                input_query(conn, query)
                                #cf.write(query + ";\n")
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
                                            name = unidecode((obj.object_name).replace(".xlsx", "") + " " + sheet)
                                            context.log.info(f"Reading file {name}")
                                            it = {
                                                'name': sanitize_db_name(name.replace(" ", "_")),
                                                'bucket_name': bucket.name,
                                                'rows': rows,
                                                'columns': columns,
                                                'lista': lista
                                            }
                                            # Función para insertar datos en la base de datos
                                            tables.update({str(i): it})
                                            i += 1
                                    # Mover el archivo procesado a una carpeta diferente
                                    minio_cli.remove_object(bucket.name, obj.object_name)
                                    #cf.close()
                                    #os.remove(os.path.abspath(cf.name))
                    cf.close()
                    minio_cli.fput_object(
                        "configuration",  # Name of the bucket
                    "configuration.sql",  # Object name in the bucket
                    os.path.abspath(cf.name)# Path to the file to upload
                    )
    except S3Error as e:
        print("Error:", e)
        return tables
    return tables


@asset(ins={"tables": AssetIn("obtain_data_from_excels")},
       group_name="Data_Integration_excel",
       required_resource_keys={"postgres"})
def transform_data(context, tables):
    """
    :param context: the context utilized during the procedure, allows us to obtain resources
    :param tables: the information passed down from the previous asset
    Its main function is to generate the tables using dictionaries obtained from iterate_lib, and saving a series
    of persistence files which we could later use in case of system malfunction
    """
    client = init_minio_server(19000, "minio", "minio123")
    postgres = context.resources.postgres
    with postgres.get_connection() as conn:
        if len(tables) != 0:
            for ele in tables.keys():
                try:
                    with open(tables[ele]['name'] + ".sql", "w") as f:
                        name = tables[ele]['name']
                        bucket = fix_string(tables[ele]['bucket_name'])
                        insert_table_to_db(conn, tables[ele]['lista'], name, tables[ele]['bucket_name'], f)

                        for row in tables[ele]['rows']:
                            columns = tables[ele]['columns']
                            filtered_row, filtered_columns = reformat_rows(row, columns)
                            query = '''insert into {}.{} ({}) values ({})'''.format(bucket, name,
                                                                                               str(filtered_columns).replace(
                                                                                                   "'",
                                                                                                   "")[
                                                                                               1:-1],
                                                                                               str(filtered_row).replace(
                                                                                                   '"',
                                                                                                   "")[
                                                                                               1:-1])
                            input_query(conn,query)
                            f.write(query + ";\n")
                        if not os.path.exists(f.name):
                            print(f"Error: File '{f.name}' not found.")
                        else:
                            client.fput_object(
                                "configuration",  # Name of the bucket
                                bucket + "/" + f.name,  # Object name in the bucket
                                os.path.abspath(f.name)  # Path to the file to uploa
                                # Absolute path of the file to upload
                            )

                        #f.close()
                        #os.remove(os.path.abspath(f.name))
                            #input_query(conn, query)
                except Exception as e:
                    context.log.error(f'Error creating schema: {e}')
                    return []

    return []


@asset(group_name="Data_Integration_csv",
       required_resource_keys={"postgres"})
def transform_csv(context):
    postgres = context.resources.postgres
    minio_cli = init_minio_server(19000, "minio", "minio123")
    buckets = minio_cli.list_buckets()
    if len(buckets) > 0:
        with postgres.get_connection() as conn:
            for bucket in buckets:
                if "configuration" not in bucket.name:
                    objs = minio_cli.list_objects(bucket.name, recursive=True)
                    fixed_bucket_name = fix_string(bucket.name)
                    for obj in objs:
                        if obj.object_name.endswith(".csv"):
                            #context.log.info(f"Processing files in bucket: {obj.object_name}")
                            response = minio_cli.get_object(bucket.name, obj.object_name)
                            csv_file = BytesIO(response.read())
                            df = pd.read_csv(csv_file, encoding='latin-1', sep='delimiter', header=None,
                                             engine='python')
                            name_farm = sanitize_db_name(fix_string(obj.object_name[:-4]))
                            with open(name_farm + ".sql", "w") as f:
                                #query = f'''create table if not exists my_catalog.{fixed_bucket_name}.{name_farm} (name_farm varchar, prefix varchar, fecha varchar, n_animales bigint, Documento_salida bigint, Extra varchar)'''
                                query = f'''create table if not exists {fixed_bucket_name}.{name_farm} (name_farm varchar, prefix varchar, fecha varchar, n_animales bigint, Documento_salida bigint, Extra varchar)'''
                                input_query(conn,query)
                                f.write(query + ";\n")

                                current_datetime = str(datetime.now()).split(".")[0]
                                #query = '''insert into my_catalog.info.files (table_name, creation) values ('{}',{})'''.format(
                                #    fixed_bucket_name + "." + name_farm,
                                #    "timestamp" + " '" + current_datetime + "'")
                                query = '''insert into info.files (table_name, creation) values ('{}',{})'''.format(
                                        fixed_bucket_name + "." + name_farm,
                                        "timestamp" + " '" + current_datetime + "'")
                                input_query(conn, query)
                                f.write(query + ";\n")

                                for index, row in df.iterrows():
                                    current_row = " ".join(row.astype(str).str.replace("\t", " "))
                                    #context.log.info(current_row)
                                    regex_formato = r'\b\d{1,2}/\d{1,2}(?:/\d{4})?\b\s+Venta\b'
                                    match = re.search(regex_formato, current_row)
                                    if "RECRIASIN" in current_row:
                                        break
                                    if match:
                                        current_row = current_row.replace("Venta", " ")
                                        dateandrow = current_row.split(sep=" ", maxsplit=1)
                                        prefix_farm = name_farm[0]
                                        date = dateandrow[0]
                                        purged_row = dateandrow[1]
                                        #context.log.info(purged_row)
                                        # pattern = r'[A-ZÍÚÓÉÁÑ][a-zíúóéáñ]*(?: [a-zíúóéáñ]+)*(?: *: *)(?:[A-ZÍÚÓÉÁÑ]+(?: [A-ZÍÚÓÉÁÑ.][A-ZÍÚÓÉÁÑ.]+)*|\d+)'
                                        pattern = r"[A-Z][a-z]*(?: [a-z]*)*(?: *: *)\d+"
                                        matches = re.findall(pattern, purged_row.strip())
                                        #context.log.info(matches)
                                        animales_bool = False
                                        docu_salida_bool = False
                                        data = f"'{name_farm}', '{prefix_farm}', '{date}', "
                                        for matchin in matches:
                                            #context.log.info(matchin)
                                            clave, valor = matchin.split(":")
                                            if "Animales" in clave:
                                                data += valor + ", "
                                                animales_bool = True
                                                #context.log.info("animales: si")
                                                purged_row = purged_row.replace(matchin, "")
                                            elif "Documento salida" in clave:
                                                data += valor + ", "
                                                docu_salida_bool = True
                                                #context.log.info("Documento salida: si")
                                                purged_row = purged_row.replace(matchin, "")
                                        if animales_bool and docu_salida_bool:
                                            weird_purged_row = purged_row
                                            data += f"'{weird_purged_row.strip()}'"
                                            query = f"insert into {fixed_bucket_name}.{str(name_farm)} (name_farm, prefix, fecha, n_animales, Documento_salida, extra) values ({data})"
                                            input_query(conn, query)
                                            f.write(query + ";\n")
                                f.close()

                                minio_cli.fput_object(
                                    "configuration",  # Name of the bucket
                                    bucket.name + "/" + f.name,  # Object name in the bucket
                                    os.path.abspath(f.name)  # Path to the file to upload
                                )

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
                corrected_column =  sanitize_db_name(unidecode(column))
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


def add_directory_to_config(folder_name, client):
    dirs = list_directories("configuration", client)
    for dir in dirs:
        if dir == folder_name:
            return False

    byte_stream = BytesIO(b'')

    client.put_object("configuration", folder_name + "/", data=byte_stream, length=0)
    return True


def list_directories(bucket_name, client):
    # Use list_objects with recursive=False to get top-level directories
    objects = client.list_objects(bucket_name, recursive=False)

    directories = set()
    for obj in objects:
        # Split object name by '/' and get the top-level directory
        parts = obj.object_name.split('/')
        if len(parts) > 1:
            directories.add(parts[0] + '/')

    return directories


def minio_mv(minio_cli, bucket_name, obj):
    minio_cli.copy_object(f"configuration", f"/{bucket_name}/" + obj.object_name, CopySource(bucket_name, obj.object_name))
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

def sanitize_db_name(name: str):
    sanitized_name = re.sub(r'[^\x00-\x7F]+', '', name)
    sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '', sanitized_name)
    max_length = 63
    if len(sanitized_name) > max_length:
        sanitized_name = sanitized_name[:max_length]
    return sanitized_name

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
    conn.commit()
    cursor.close()
    return


def check_if_schema_exists(conn ,schema):

    query = f"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '{schema}');"
    cursor = conn.cursor()
    cursor.execute(query)
    tables = cursor.fetchone()
    conn.commit()
    cursor.close()
    return tables

def insert_table_to_db(conn, lista, name, bucket, file):
    current_datetime = str(datetime.now()).split(".")[0]
    columns_definition = ', '.join([f'{col[0]} {col[1]}' for col in lista])

    #query = '''create table if not exists my_catalog.{}.{} ({})'''.format(bucket, name, columns_definition)
    query = '''create table if not exists {}.{} ({})'''.format(bucket, name,columns_definition)
    input_query(conn, query)
    file.write(query + ";\n")
    #query = '''insert into my_catalog.info.files (table_name, creation) values ('{}',{})'''.format(
    #    bucket + "." + name, "timestamp" + " '" + current_datetime + "'")
    query = '''insert into info.files (table_name, creation) values ('{}',{})'''.format(
            bucket + "." + name, "timestamp" + " '" + current_datetime + "'")
    input_query(conn, query)
    file.write(query + ";\n")

def create_info_table(conn, file):
    query = '''create schema if not exists info'''
    input_query(conn, query)
    file.write(query + ";\n")
    #query = '''create table if not exists my_catalog.info.files (table_name varchar,creation TIMESTAMP)'''
    query = '''create table if not exists info.files (table_name varchar,creation TIMESTAMP)'''
    input_query(conn, query)
    file.write(query + ";\n")

def init_minio_server(port: int, name: str, password: str):
    client = Minio(
        f"host.docker.internal:{port}",
        access_key=name,
        secret_key=password,
        secure=False
    )
    return client



