import pandas as pd
import re
import os
import sys
sys.path.insert(0, r"examplecode\python\functions")
from functions.synserverlessobjects import *

def create_df_from_repo(repo_path, base_wd, localind = 'False'):
    
    if localind == 'True':
        path = repo_path
    else:
        path = os.getcwd() +'\\' +  os.path.basename(repo_path)
            
    #w store all the file names in a list
    filelist = []

    for root, dirs, files in os.walk(path):
        for file in files:
        #append the file name to the list
            filelist.append(os.path.join(root,file))

    #initiate pandas dataframe from filelist
    df_repo = pd.DataFrame({'file':filelist})

    df_repo['short'] = df_repo['file'].str.replace(base_wd, '', regex=False, flags=re.I)

    # get database name
    df_repo['DBName'] = df_repo['short'].str.split('\\').str[0]

    # get schema name
    df_repo['SchemaName'] = df_repo['short'].str.split('\\').str[1]
    df_repo['SchemaName'] = df_repo['SchemaName'].str.split('.').str[0]

    # get view name
    df_repo['ObjectName'] = df_repo['short'].str.split('.').str[-2]

    df_repo = df_repo.drop('short', axis=1)

    # get file contents
    for index, file in df_repo.iterrows():
        f = open(file['file'], "r")
        df_repo.at[index, 'ViewDefinition'] = f.read()
        f.close()

    # clean sql statement 
    df_repo['definition'] = df_repo['definition'].transform(clean_sql_func_df)

    df_repo['Env'] = "repo" 

    return df_repo


def create_target_df(sql, cnxn, cursor, database, objecttype):
    
    cursor.execute("USE [" + database + "];")
 
    df = pd.read_sql(sql, cnxn)
    df['objectType'] = objecttype

    return  df


def merge_dataframes(df_repo, df_target):
    # join dataframes 
    df_merged = df_repo.merge(df_target, how='outer', on=['DBName', 'SchemaName', 'ObjectName'])

    df_merged['definition_y'] = df_merged['definition_y'].str.replace(r'\r', '', regex=True)
    df_merged['definition_x'] = df_merged['definition_x'].str.replace(r'\r', '', regex=True)
    df_merged.loc[df_merged['definition_y'].str[-1] == ';', 'definition_y' ] = df_merged['definition_y'].str[:-1].str.replace(r';$', '', regex=True, flags=re.M)
    df_merged.loc[df_merged['definition_x'].str[-1] == ';', 'definition_x' ] = df_merged['definition_x'].str[:-1].str.replace(r';$', '', regex=True, flags=re.M)
    df_merged['compare_sql'] = ( df_merged['definition_x'] == df_merged['definition_y'] )

    df_merged['len_x'] = df_merged['definition_x'].str.len()
    df_merged['len_y'] = df_merged['definition_y'].str.len()

    return df_merged


def clean_and_prepare(df_merged):

    df_lean = df_merged[['DBName', 'SchemaName', 'ObjectName', 'objectType', 'definition_x', 'compare_sql' ]]
    df_lean = df_lean.query('compare_sql == False')
    df_lean = df_lean.rename(columns={'definition_x':'Execute_Statement'})

    df_lean.loc[df_lean['Execute_Statement'].notnull(), 'Execute_Statement'] = df_lean['Execute_Statement'] + ';'
    df_lean.loc[df_lean['Execute_Statement'].notnull(), 'Execute_Statement'] = df_lean['Execute_Statement'].str.replace(r"BULK 'replacedforcomparison',$", " BULK 'bronze/dummy/dummyfile.parquet',", regex=True, flags=re.M)
    df_lean.loc[(df_lean['Execute_Statement'].isnull()) & ((df_lean['objectType'] == 'VIEW') | (df_lean['objectType'] == 'PROCEDURE')), 'Execute_Statement'] = 'DROP '+ df_lean['objectType'] + ' IF EXISTS [' + df_lean['SchemaName'] + '].[' + df_lean['ObjectName'] + '] \n;'
    df_lean.loc[(df_lean['Execute_Statement'].isnull()) & (df_lean['objectType'] == 'EXTERNAL TABLE'), 'Execute_Statement'] = 'IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N['+ df_lean['SchemaName'] + '].[' + df_lean['ObjectName'] + ']) AND type in (N''U'')) DROP '+ df_lean['objectType'] + '[' + df_lean['SchemaName'] + '].[' + df_lean['ObjectName'] + '];'

    df_lean = df_lean[['DBName', 'SchemaName', 'ObjectName', 'Execute_Statement', 'compare_sql' ]]

    return df_lean
    