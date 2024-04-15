import os
import re
import shutil
from contextlib import contextmanager
from datetime import datetime

import pandas as pd
from dagster import (InputDefinition, ModeDefinition, asset, build_op_context,
                     job, op, pipeline, repository, resource, solid)
from dagster.utils.yaml_utils import load_yaml_from_path
from dagster_dbt import dbt_cli_resource
from dagster_pyspark import pyspark_resource
from trino.dbapi import connect


class TrinoConnection:

    def __init__(self, connection_params):
        self._connection_params = connection_params

    @contextmanager
    def get_connection(self):
        conn = connect(host=self._connection_params["host"], port=self._connection_params["port"],
                       user=self._connection_params["user"])
        yield conn
        conn.close()

@resource(config_schema={"host": str, "port": str, "user": str, "password": str})
def trino_resource(init_context):
    connection_params = {
        "host": init_context.resource_config["host"],
        "port": init_context.resource_config["port"],
        "user": init_context.resource_config["user"],
        "password": init_context.resource_config["password"]
    }
    return TrinoConnection(connection_params)
@asset
def iterate_lib(context):
    
    path = "/var/lib/ngods/dagster/input_files"
    tables = {}
    for ele in os.listdir(path):
        it = {}
        if ele.endswith(".xlsx"):
            for sheet in pd.ExcelFile(path + "/" +str(ele)).sheet_names:
                lista,rows,columns = read_files_op(context,path + "/" +str(ele),str(sheet))
                if len(lista) != 0:
                    it = {
                        'name_file':ele.replace(" ","_").replace(".xlsx",""),
                        't_create':lista,
                        'rows':rows,
                        'columns':columns
                    }
                    name = str(ele).replace(".xlsx", "").replace(" ","_")
                    tables[sheet] = it
            shutil.move(path + "/" + str(ele), "/var/lib/ngods/dagster/processed_files/")
    return tables
            
def read_files_op(context,path,sheet):
    try:
        df = pd.read_excel(path,sheet_name=sheet)
        if not df.isnull().all().any() and not df.empty: 
            lista = []
            rows = []
            columns  = []
            for row in df.itertuples(index = False):
                rows.append(list(row))
            for nombre_columna,valor_columna in zip(df.iloc[0].index,df.iloc[0]):
                if "%" in nombre_columna:
                    nombre_columna = nombre_columna.replace("%","porcentaje_")
                lista.append([nombre_columna,identify_string_type(str(valor_columna))])
                columns.append(nombre_columna)
            return lista,rows,columns
        else:
            return [],[],[]
    except Exception as e:
        context.log.error(f"Error al leer el archivo {path}: {str(e)}")
        raise e

def identify_string_type(input_string):
    # Regular expressions for matching different types
    timestamp_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$'
    decimal_pattern = r'^-?[0-9]+(\.[0-9]+)?$'
    integer_pattern = r'^-?[0-9]+$'
    # Check if it's a timestamp
    if re.match(timestamp_pattern, input_string) is not None:
        return "Timestamp(0)"
    # Check if it's a double
    elif re.match(decimal_pattern, input_string.replace(",","")) is not None:
        return "Double"
    # Check if it's an integer
    elif re.match(integer_pattern, input_string.replace(",","")) is not None:
        return "Bigint"
    # Otherwise, it's just a string
    else:
        return "varchar"
    
@op(required_resource_keys={'trino'})
def init(context,tables):
    trino = context.resources.trino
    with trino.get_connection() as conn:
        if os.listdir("/var/lib/ngods/dagster/launch"):
            open_persisted_queries(conn,"/var/lib/ngods/dagster/launch/struct.sql")
            for ele in os.listdir("/var/lib/ngods/dagster/launch"):
                if ele != "struct.sql":
                    open_persisted_queries(conn,"/var/lib/ngods/dagster/launch/" + ele)
        if len(tables) != 0:
            for ele in tables:
                context.log.info(tables[ele]['name_file'])
            try:
                with open("/var/lib/ngods/dagster/launch/struct.sql", "a") as f1:
                    query = """create schema if not exists my_catalog.integracion"""
                    f1.write(query + "\n")
                    input_query(conn,query)
                    query = "create table if not exists my_catalog.integracion.files (table_name varchar,creation TIMESTAMP)"
                    f1.write(query + "\n")
                    input_query(conn,query)  
                    for ele in tables:
                        lista = tables[ele]['t_create']
                        columns_definition = ', '.join([f'{col[0]} {col[1]}' for col in lista])
                        name = tables[ele]['name_file'] + "_" + ele
                        query = '''create table if not exists my_catalog.integracion.{} ({})'''.format(name,columns_definition)
                        f1.write(query + "\n")
                        input_query(conn,query)    
                        current_datetime = str(datetime.now()).split(".")[0]
                        query = '''insert into my_catalog.integracion.files (table_name, creation) values ('{}',{})'''.format(name,"timestamp" + " '" + current_datetime + "'")
                        f1.write(query + "\n")
                        input_query(conn,query)
                        with open("/var/lib/ngods/dagster/launch/" + name + ".sql", "a") as f2:
                            for row in tables[ele]['rows']:
                                columns = tables[ele]['columns']
                                filtered_row,filtered_columns = reformat_rows(row,columns)
                                query = '''insert into my_catalog.integracion.{} ({}) values ({})'''.format(name,str(filtered_columns).replace("'","")[1:-1],str(filtered_row).replace('"',"")[1:-1])
                                f2.write(query + "\n")
                                input_query(conn,query)
                            f2.close()
            except Exception as e:
                context.log.error(f'Error creating schema: {e}')
    return []

def reformat_rows(row,columns):
    row_copy = row.copy()
    columns_copy = columns.copy()
    count = 0
    for pos,ele in enumerate(row):
        tipo = identify_string_type(str(ele))
        if tipo == "Timestamp(0)" and not "datetime.datetime" in str(ele) and not "datetime.time" in str(ele):
            value = "Timestamp '" + str(ele) + "'"
            row_copy.pop(pos - count)
            row_copy.insert(pos - count,value)
        elif tipo == "Timestamp(0)" and "datetime.datetime" in str(ele) and not "datetime.time" in str(ele):
            value = row_copy.pop(pos - count)
            numbers = str(value).replace("datetime.datetime(","").replace(")","").split(", ")
            numbers_copy = numbers.copy()
            for pos, ele in enumerate(numbers_copy):
                if ele == "0":
                    numbers.pop(pos)
                    numbers.insert(pos,"00")
            while len(numbers) < 6:
                numbers.append("00")
            if len(numbers) > 6:
                row_copy.insert(pos - count,f"TIMESTAMP '{numbers[0]}-{numbers[1]}-{numbers[2]} {numbers[3]}:{numbers[4]}:{numbers[5]}.{numbers[6]}'")
            else:
                row_copy.insert(pos - count,f"TIMESTAMP '{numbers[0]}-{numbers[1]}-{numbers[2]} {numbers[3]}:{numbers[4]}:{numbers[5]}'")
        
        elif tipo == "Timestamp(0)" and not "datetime.datetime" in str(ele) and  "datetime.time" in str(ele): 
            row_copy.pop(pos - count)
            columns_copy.pop(pos - count)
            count += 1
        elif tipo == "varchar" and (str(ele) == "nan" or str(ele) == "NaT"):
            row_copy.pop(pos - count)
            columns_copy.pop(pos - count)
            count += 1
        
    return row_copy,columns_copy
        
def input_query(conn,query):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.fetchall()
    conn.commit()
    
    return

def open_persisted_queries(conn,path):
    with open(path, "r") as file:
        f=file.read()
        for query in f.split("\n")[:-1]:
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.fetchall()
            conn.commit()
            
@repository
def workspace():
    config = {
        "resources": {
            "trino": {
                "config": {
                    "host": "trino",
                    "port": "8060",
                    "user": "trino",
                    "password": "",
                }
            }
        }
    }
    resource_config = config.get("resources", {}).get("trino", {}).get("config", {})
    
    with build_op_context(resources={'trino': trino_resource.configured(resource_config)}) as con:
        return [init(con,iterate_lib(con))]
        #return [create(con)]