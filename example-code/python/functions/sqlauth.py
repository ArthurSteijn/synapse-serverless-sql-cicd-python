import pyodbc
import re
import struct
import argparse

def get_user_input():
    username = str(input('enter your username (emailaddress): '))
    
    if not validate_email(username):
        print("Invalid email") 
        username = str(input('no valid email; enter your username (emailaddress): '))
    
    output_directory = str(input('enter the absolute path for the output directory (i.e. C:\..\syn-serverless-objects): '))
    
    with open('local_settings.local', 'w') as f:
        f.write(username + '\n')
        f.write(output_directory)
        f.close()

    return username, output_directory


def get_personal_settings():
    try:
        params = open('local_settings.local', 'r')
        lines = params.readlines()
        params.close()
        
        username = lines[0].strip()
        output_directory = lines[1].strip()

        if not validate_email(username):
            print("Invalid email, try again") 
            username, output_directory = get_user_input()

    except:
        print('File not found, enter some parameters')
        username, output_directory =  get_user_input()

    return username, output_directory


def validate_email(email):
    if len(email) > 7:
        if re.match("^.+@.+\..+$", email):
            return True
        else:
            return False
    else:
        return False


def pass_arguments():
    # Construct an argument parser  
    all_args = argparse.ArgumentParser()

    # Add arguments to the parser
    all_args.add_argument('-synservername', '--synservername', required=True, 
    help="Specify the Target sql server (rightside of comparison) e.g. <name>-ondemand.sql.azuresynapse.net")
    all_args.add_argument('-access_token', '--accesstoken', required=True,
    help="sql server acces token")
    args = vars(all_args.parse_args())

    return args


def dbcnxn(server, username):
    database = 'master'
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';Authentication=ActiveDirectoryInteractive') 
    cursor = cnxn.cursor()

    return cursor, cnxn


def use_token(server, token):
    connection_string = 'Driver={ODBC Driver 17 for SQL Server};server='+server+';database=master;'
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    
    exptoken = b''
    for i in bytes(token, "UTF-8"):
        exptoken += bytes({i})
        exptoken += bytes(1)
    tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
    cnxn = pyodbc.connect(connection_string, attrs_before = { SQL_COPT_SS_ACCESS_TOKEN:tokenstruct })
    cursor = cnxn.cursor()

    return cursor, cnxn