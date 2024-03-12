from dagster import job, repository, ModeDefinition
from dagster_dbt import dbt_cli_resource
from dagster_pyspark import pyspark_resource

import os
import pandas as pd
import shutil
from datetime import datetime

import re
from dagster import job, op,solid, resource,build_op_context, InputDefinition,pipeline, ModeDefinition, asset
from trino.dbapi import connect
from contextlib import contextmanager
from dagster.utils.yaml_utils import load_yaml_from_path

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
    i = 0
    tables = {}
    for ele in os.listdir(path):
        it = {}
        if ele.endswith(".xlsx"):
            for sheet in pd.ExcelFile(path + "/" +str(ele)).sheet_names:
                context.log.info(f"Reading file {ele} and sheet {str(sheet)}")
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
    timestamp_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    decimal_pattern = r'^-?[0-9]+(\.[0-9]+)?$'
    integer_pattern = r'^-?[0-9]+$'
    # Check if it's a timestamp
    if re.match(timestamp_pattern, input_string) is not None:
        return "Timestamp(0)"
    # Check if it's a double
    elif re.match(decimal_pattern, input_string.replace(",","")) is not None:
        sp = input_string.split(".")
        return "Double"
    # Check if it's an integer
    elif re.match(integer_pattern, input_string.replace(",","")) is not None:
        return "Bigint"
    # Otherwise, it's just a string
    else:
        return "varchar"


@op(required_resource_keys={'trino'})
def init(context,tables):
    query_list = []
    trino = context.resources.trino
    with trino.get_connection() as conn:
        if os.listdir("/var/lib/ngods/dagster/launch"):
            for ele in os.listdir("/var/lib/ngods/dagster/launch"):
                open_persisted_queries(conn,"/var/lib/ngods/dagster/launch/" + ele)    
        if len(tables) != 0:
            try:
                query = """create schema if not exists my_catalog.integracion"""
                input_query(conn,query,query_list)
                query = "create table if not exists my_catalog.integracion.files (table_name varchar,creation TIMESTAMP)"
                input_query(conn,query,query_list)
                for ele in tables:
                    query_list = []
                    lista = tables[ele]['t_create']
                    columns_definition = ', '.join([f'{col[0]} {col[1]}' for col in lista])
                    name = tables[ele]['name_file'] + "_" + ele
                    query = '''create table if not exists my_catalog.integracion.{} ({})'''.format(name,columns_definition)
                    input_query(conn,query,query_list)
                    columns = str(tables[ele]['columns'])[1:-1].replace("'","")
                    context.log.info("{}".format(str(datetime.now()).split(".")[0]))
                    query = '''insert into my_catalog.integracion.files (table_name,creation) values ('{}',CURRENT_TIMESTAMP)'''.format(name)
                    input_query(conn,query,query_list)
                    for row in tables[ele]['rows']:
                        values = str(row)[1:-1].replace("nan","0").replace("NaT","Null").replace("Timestamp(","TIMESTAMP ").replace(")","")
                        #context.log.info(f"Inserting values {values}")
                        query = '''insert into my_catalog.integracion.{} ({}) values ({})'''.format(name,columns,values)
                        input_query(conn,query,query_list)
                    persist_query_list(query_list,"/var/lib/ngods/dagster/launch/{}.sql".format(name))
            except Exception as e:
                context.log.error(f'Error creating schema: {e}')
    return []

def input_query(conn,query,query_list):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.fetchall()
    conn.commit()
    query_list.append(query)
    return

def persist_query_list(query_list,path):
    with open(path, "a") as file:
        for query in query_list:
            file.write(query + "\n")
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