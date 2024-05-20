import os
from datetime import datetime

import pandas as pd
import shutil

import re
from dagster import asset, AssetIn

import json


@asset(group_name="Data_Integration_excel")
def obtain_data_from_excels(context):
    """
    Parses the different files found in input_files, generates the minimum info for init to work
    """
    path = '/opt/dagster/app/input_files'
    i = 0
    tables = {}
    for ele in os.listdir(path):
        if ele.endswith(".xlsx"):
            for sheet in pd.ExcelFile(path + "/" + str(ele)).sheet_names:
                context.log.info(f"Reading file {ele} and sheet {str(sheet)}")
                lista, rows, columns = read_files_op(context, path + "/" + str(ele), str(sheet))
                if len(lista) != 0:
                    it = {
                        'name_file': fix_string(ele).replace("xlsx", ""),
                        'sheet_name': fix_string(sheet),
                        't_create': lista,
                        'rows': rows,
                        'columns': columns
                    }
                    tables[i] = it
                    i += 1
        shutil.move(path + "/" + str(ele), "/opt/dagster/app/processed_files")

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
            try:
                with open("/opt/dagster/app/launch/struct.sql", "a", encoding="UTF-8") as f1:
                    query = '''create schema if not exists my_catalog.integracion'''
                    f1.write(query + "\n")
                    input_query(conn, query)
                    query = '''create table if not exists my_catalog.integracion.files (table_name varchar,creation TIMESTAMP)'''
                    f1.write(query + "\n")
                    input_query(conn, query)
                    for ele in tables:
                        lista = tables[ele]['t_create']
                        columns_definition = ', '.join([f'{col[0]} {col[1]}' for col in lista])
                        name = tables[ele]['name_file'] + "_" + tables[ele]['sheet_name']
                        query = '''create table if not exists my_catalog.integracion.{} ({})'''.format(name,
                                                                                                       columns_definition)
                        f1.write(query + "\n")
                        input_query(conn, query)
                        current_datetime = str(datetime.now()).split(".")[0]
                        query = '''insert into my_catalog.integracion.files (table_name, creation) values ( '{}', {} )'''.format(
                            name, "timestamp" + " '" + current_datetime + "'")
                        f1.write(query + "\n")
                        input_query(conn, query)
                        with open("C:\\laburo\\assets_stelviotech\\assets\\launch\\" + name + ".sql", "a", encoding="UTF-8") as f2:
                            for row in tables[ele]['rows']:
                                columns = tables[ele]['columns']
                                filtered_row, filtered_columns = reformat_rows(row, columns)
                                query = '''insert into my_catalog.integracion.{} ({}) values ({})'''.format(name,
                                                                                                            str(filtered_columns).replace(
                                                                                                                "'",
                                                                                                                "")[
                                                                                                            1:-1],
                                                                                                            str(filtered_row).replace(
                                                                                                                '"',
                                                                                                                "")[
                                                                                                            1:-1])
                                f2.write(query + "\n")
                                input_query(conn, query)
                            f2.close()
            except Exception as e:
                context.log.error(f'Error creating schema: {e}')
    return []


@asset(group_name="Db_Functions",
required_resource_keys={"trino"})
def launch_db_from_files(context):
    """
    :param context: context employed during the procedure, allows us to obtain resources
    Its main objective is to Regenerate the tables using the files stored in launch
    """
    trino = context.resources.trino
    with trino.get_connection() as conn:
        if os.listdir("/opt/dagster/app/launch"):
            if len(os.listdir("/opt/dagster/app/launch")) > 1:
                open_persisted_queries(conn, "/opt/dagster/app/launch/struct.sql")
                for name in os.listdir("/opt/dagster/app/launch"):
                    if name != "struct.sql":
                        open_persisted_queries(conn, "/opt/dagster/app/launch/" + name)
    return []


@asset(group_name="Data_Integration_json")
def obtain_data_from_jsons(context):
    path = '/opt/dagster/app/input_files'
    jsons = []
    if os.listdir(path):
        for ele in os.listdir(path):
            if ele.endswith(".json"):
                context.log.info(f"Reading file {ele}")
                json_file = open(path + "/" + ele, "r", encoding="UTF-8")
                jsons.append(json.load(json_file))
    return jsons


@asset(ins={"jsons": AssetIn("obtain_data_from_jsons")},
group_name="Data_Integration_json",
required_resource_keys={"trino"})
def transform_json(context, jsons):
    trino = context.resources.trino
    with trino.get_connection() as conn:
        max_len = 5
        tables = []
        with (open("/opt/dagster/app/launch/struct.sql", "a", encoding="UTF-8")) as struct:
            for ele in jsons:
                context.log.info(f"Reading {ele}")
                company_initials = ""
                if isinstance(ele['products'][0]['company']['name'], str):
                    company_name = fix_string(ele['products'][0]['company']['name'])
                    query = '''create schema if not exists my_catalog.{}'''.format(company_name)
                    struct.write(query + "\n")
                    context.log.info(f"First query {query}")
                    input_query(conn, query)
                    count = 0
                    for word in str(company_name).split("_"):
                        count += 1
                        if count == max_len:
                            break
                        company_initials += word[0]
                    for workflow in ele['workflowSteps']:
                        workflow_name = workflow['name']
                        wds_data_names = []
                        for workflow_step_data_field in workflow['workflowStepDataFields']:
                            wds_data_names.append((workflow_step_data_field['name'], workflow_step_data_field[
                                'workflowDataType']))
                        if len(wds_data_names) > 0:
                            table_name = fix_string(company_initials + "_" + workflow_name)
                            tables.append((table_name, wds_data_names))
                            query = '''create table if not exists my_catalog.{}.{} ({})'''.format(company_name,
                                                                                                  table_name,
                                                                                                  apply_column_structure(
                                                                                                      str(wds_data_names)
                                                                                                      [1:-1]))
                            struct.write(query + "\n")
                            context.log.info(f"Second query {query}")
                            input_query(conn, query)

    context.log.info("Variable tables contains: {}".format(tables))
    return tables


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
                rows.append(list(row))
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
                context.log.info(f"From sheet {sheet} - {column} - {column_value}")
                columns.append(corrected_column)
            return lista, rows, columns
        else:
            return [], [], []
    except Exception as e:
        context.log.error(f"Error al leer el archivo {path}: {str(e)}")
        raise e


def identify_string_type(input_string):
    # Regular expressions for matching different types
    timestamp_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$'
    decimal_pattern = r'^[0-9]+(\.[0-9]+)$'
    integer_pattern = r'^[0-9]+$'
    boolean_patterns = ['true', 'True', 'TRUE', 'False', 'false', 'FALSE']
    # Check if it's a timestamp
    if re.match(timestamp_pattern, input_string) is not None:
        return "Timestamp(0)"
    # Check if it's a double
    elif re.match(decimal_pattern, input_string.replace(",", "")) is not None:
        return "Decimal"
    # Check if it's an integer
    elif re.match(integer_pattern, input_string.replace(",", "")) is not None:
        return "bigint"
    # Otherwise, it's just a string
    elif input_string.replace(",", "") in boolean_patterns:
        return "boolean"
    else:
        return "varchar"


def fix_string(string):
    special_characters = [
        ';', '--', '/*', '*/', "'", '"', '\\', '%', '_', '<', '>', '=', '+', '-', '*', '/', '@', '#', '!', '~', '`', '|'
        , '&', '^', '$', '?', '(', ')', '[', ']', '{', '}', ',', '.', ':', ' '
    ]
    modi_string = string
    for char in special_characters:
        if char == '%':
            modi_string = modi_string.replace(char, "porcentaje_")
        else:
            modi_string = modi_string.replace(char, '_')
            if '__' in modi_string:
                modi_string = modi_string.replace('__', '_')

    return modi_string


def apply_column_structure(string):
    return string.replace('(', ' ').replace(')', '')


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
    cursor.fetchall()
    conn.commit()
    return


def open_persisted_queries(conn, path):
    with open(path, "r", encoding="UTF-8") as file:
        f = file.read()
        for query in f.split("\n")[:-1]:
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.fetchall()
            conn.commit()

