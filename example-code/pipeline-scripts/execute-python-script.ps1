#This script is used to deploy the SQL scripts that are saved in the REPO to the SQL Serverless Database
#Note that databases and schemas must exist, and are not created automatically 
param(
   [string][parameter(Mandatory = $true)] $ServerInstance,
   [string][parameter(Mandatory = $true)] $execute_var
)

#Import needed modules
Write-Host("Importing modules")
Import-Module SQLServer
Import-Module Az.Accounts -MinimumVersion 2.2.0

#Get an access token with the Service Pricipal used in the Azure DevOps Pipeline
Write-Host("Get Access Token")
$access_token = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token

#Install Python Modules
Write-Host("Installing Python Modules")
python -m pip install pyodbc
python -m pip install pandas

#Start Python Script
Write-Host("Starting python script")
& python.exe 'directory/compare-and-deploy-database-changes.py' -synserver $ServerInstance -access_token $access_token -execute_var $execute_var
if ( $LASTEXITCODE -eq 1)
{
      Write-Error "Script Execution Failed"
}