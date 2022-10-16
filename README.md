# Azure Synapse Serverless SQL CI/CD example in Python and Pandas

This repo contains the slides as presented on Data Relay 2022 (2022-10-05 Birmingham)

As well as example code to reproduce the solution.

Use of Visual Studio Code with Python and Jupyter Notebook extensions is recommended.

**Please note** that this is a tailor made solution and will probably need some adjustments to work in other scenarios.

I will keep on working on my solution to add more features and add somemore error handeling. Next up are External Tables and a fix for not existing references in the Data Lake.

If you have any improvements that you would like to share, please do so!

## Example code

Short explanation of what can be found within te sample code

### **01-export-views-to-sql-files.ipynb**

Use this notebook to export all sql objects to a local folder. These files can be added to the repository for source control purposes. Objects are stored in .sql files.

### **02-execute-sql-on-serverless.ipynb**

Use this notebook to execute the .sql files against a Synapse Serverless SQL pool. Files are executed in an alphabetical order. If you need a specific order of execution, i.e. based on schemas, one solution is adding this order to the export notebook!

### **03-compare-and-execute-sql-on-serverless.ipynb**

Use this notebook to do an incremental update of your Target server. With in this notebook a Pandas Dataframe is created, that compares the Repo with the Target server. 

Only sql objects with actual changes will be executed on the Target Server. This includes drop statements for Objects that are no longer within the repository

!!! important here is to look at Environment specific references that need to be replaced before comparing. In example a reference to a Development server in the repository versus the reference to a Production server in the target.

### **Azure DevOps**

Script 02 and 03 can be run from an Azure DevOps Pipeline. See 'pipeline-scripts' for an example yaml file and PowerShell file that you could use in your pipeline. The Jupyter Notebooks will have to be converted to .py files as well. 
