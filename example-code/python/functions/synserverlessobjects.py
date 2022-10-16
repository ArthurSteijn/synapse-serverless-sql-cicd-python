import pyodbc
import pandas as pd
import re
import os
import shutil
import struct
import argparse


def create_dir(path):
    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    # Create a new directory because it does not exist 
    if not isExist: 
        os.makedirs(path)


def clean_dir(path):
    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    # Delete directory if it existss
    if isExist: 
        shutil.rmtree(path)


def get_databases(cursor):
    cursor.execute("""
                    SELECT name FROM sys.databases WHERE name NOT IN ('master', 'default');
                """) 
    databases = cursor.fetchall()
    
    return databases
    

def clean_sql_func(query):
    query = re.sub(r'/\*\*\*\*\*\* Object.*',  '', query, flags=re.I)
    query = re.sub(r'CREATE+\s*VIEW', 'CREATE OR ALTER VIEW', query, flags=re.I)
    query = re.sub(r'CREATE *VIEW', 'CREATE OR ALTER VIEW', query, flags=re.I)  

    query = query.lstrip()
    
    return query


def clean_sql_func_df(column):
    column = column.str.replace(r'/\*\*\*\*\*\* Object.*',  '', regex=True, flags=re.I)
    column = column.str.replace(r'/\*\*\*\*\*\* Script.*',  '', regex=True, flags=re.I)
    column = column.str.replace(r'CREATE+\s*VIEW', 'CREATE OR ALTER VIEW', regex=True, flags=re.I)
    column = column.str.replace(r'CREATE *VIEW', 'CREATE OR ALTER VIEW', regex=True, flags=re.I) 
    column = column.str.replace(r'CREATE+\s*PROCEDURE', 'CREATE OR ALTER PROCEDURE', regex=True, flags=re.I)
    column = column.str.replace(r'CREATE *PROCEDURE', 'CREATE OR ALTER PROCEDURE', regex=True, flags=re.I)
    column = column.str.replace(r'CREATE+\s*FUNCTION', 'CREATE OR ALTER FUNCTION', regex=True, flags=re.I)
    column = column.str.replace(r'CREATE *FUNCTION', 'CREATE OR ALTER FUNCTION', regex=True, flags=re.I)    
    column = column.str.strip()
 
    return column


def get_queries():

    # SQL Query to get views
    sql_views = ( """ 
                    SELECT 
	                    DB_NAME() AS DBName
	                    ,s.name
	                    ,vw.name
	                    ,m.definition
                    FROM sys.views vw
                    LEFT JOIN sys.schemas s
	                    ON s.schema_id = vw.schema_id
                    LEFT JOIN sys.sql_modules m
	                    ON m.object_id = vw.object_id
                    """)
    # SQL Query to get stored procedures 
    sql_sps = (""" 
                    SELECT 
                         DB_NAME() AS DBName
	                    ,s.name
	                    ,p.name
	                    ,m.definition
                    FROM sys.procedures p
                    LEFT JOIN sys.schemas s
	                    ON s.schema_id = p.schema_id
                    LEFT JOIN sys.sql_modules m
	                    ON m.object_id = p.object_id
                    """)

    sql_fn = (""" 
                    SELECT 
                            DB_NAME() AS DBName
                            ,s.name
                            ,f.name
                            ,m.definition
                    FROM sys.objects f
                    JOIN sys.sql_modules as m 
                        ON m.object_id = f.object_id
                    JOIN sys.schemas s
                        ON s.schema_id = f.schema_id
                    WHERE type IN ('FN', 'IF', 'TF')  -- scalar, inline table-valued, table-valued
                    """)

    return sql_views, sql_sps, sql_fn


def export_views_to_dotsql(cursor, sqlscript_directory, database):

    sql_views, sql_sps, sql_fn = get_queries()

    cursor.execute("USE [" + database + "];")
    cursor.execute(sql_views)                                 

    records = cursor.fetchall()

    for row in records:
        table_catalog       = row[0]
        table_schema        = row[1]
        table_name          = row[2]
        view_definition     = row[3]

        path = sqlscript_directory+'/'+database +'/'
        create_dir(path)

        # clean the sql statement
        cleaned_view = clean_sql_func(view_definition)

        # write the cleaned sql to a file
        with open(sqlscript_directory+'/'+table_catalog+'/'+table_schema+'.'+table_name+'.sql', 'w', newline='', encoding='utf-8') as f:
            f.write(cleaned_view)
            f.close()


def export_sp_to_dotsql(cursor, sqlscript_directory, database):
    
    sql_views, sql_sps, sql_fn = get_queries()

    cursor.execute("USE [" + database + "];")
    cursor.execute(sql_sps) 
    
    records = cursor.fetchall()

    for row in records:
        sp_catalog       = row[0]
        sp_schema        = row[1]
        sp_name          = row[2]
        sp_definition    = row[3]

        sp_definition = re.sub(r'CREATE *PROCEDURE', 'CREATE OR ALTER PROCEDURE', sp_definition, flags=re.I)

        path = sqlscript_directory+'/'+database +'/'
        create_dir(path)

        with open(sqlscript_directory+'/'+sp_catalog+'/'+sp_schema+'.'+sp_name+'.sql', 'w', newline='', encoding='utf-8') as f:
            f.write(sp_definition)
            f.close()


def export_fn_to_dotsql(cursor, sqlscript_directory, database):
    
    sql_views, sql_sps, sql_fn = get_queries()

    cursor.execute("USE [" + database + "];")
    cursor.execute(sql_fn) 
    
    records = cursor.fetchall()

    for row in records:
        fn_catalog       = row[0]
        fn_schema        = row[1]
        fn_name          = row[2]
        fn_definition    = row[3]

        fn_definition = re.sub(r'CREATE *FUNCTION', 'CREATE OR ALTER FUNCTION', fn_definition, flags=re.I)

        path = sqlscript_directory+'/'+database +'/'
        create_dir(path)

        with open(sqlscript_directory+'/'+fn_catalog+'/'+fn_schema+'.'+fn_name+'.sql', 'w', newline='', encoding='utf-8') as f:
            f.write(fn_definition)
            f.close()

def execute_statement (cursor, database, statement):

    try:
        cursor.execute("USE [" + database + "];")
        cursor.execute(statement) 
        cursor.commit()
        print("INFO: Statement execution succesfull.")
    except Exception as e:
        print('Error Statement execution failed: ', e)
        # raise  # re-raise unexpected exception