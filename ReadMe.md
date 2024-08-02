# Data Storage Script

This script is designed to download JSON files from an FTP server, extract them, upload them to Snowflake, and transform the data into structured tables.

## Prerequisites
Before running the script, make sure you have the following:

- Python 3.6+
- Snowflake Python Connector
- FTP access credentials
- Setup
- Install Required Libraries:

Make sure you have the required Python libraries installed. You can use pip to install them:

``` sh 
pip install snowflake-connector-python
```

## FTP and Snowflake Credentials:

Ensure you have the correct FTP and Snowflake credentials and update the script accordingly.

## Script Arguments:

The script requires three arguments:

- directory: The FTP directory to download from.
- source_schema: The source schema in Snowflake to copy tables from.
- target_schema: The target schema in Snowflake to move tables to.

## Script Description
Functions:
- download_ftp_files(directory): Downloads files from the specified FTP directory.
- extract_zip_files(zip_files): Extracts ZIP files to a local directory.
- create_table_for_json(json_file, target_schema): Creates a Snowflake table based on the JSON file schema.
- upload_to_snowflake(file): Uploads a file to Snowflake stage.
- load_data_into_table(file, target_schema, table_name): Loads data into a Snowflake table from the uploaded file.
- backup_and_copy_tables(source_schema, target_schema): Backs up and copies tables from the source schema to the target schema.
- create_full_vehicle_table(schema_name, combined_table_name): Creates the FULL_VEHICLE table with the necessary joins.
- move_full_vehicle_to_references(source_schema, table_name): Moves the FULL_VEHICLE table to the REFERENCES schema.
## Script Flow
- Download Data:
- The script downloads the data files from the FTP server.
- Extract Data:
- Extract the downloaded ZIP files.
- Process Data:
- Create Snowflake tables based on the JSON schema and upload the data to Snowflake.
- Backup and Copy Tables:
- Backup the current tables and copy the new tables to the target schema.
- Move Table to References Schema:
- Create Full Vehicle Table:
- Create the FULL_VEHICLE table by joining various views and tables.
- Move the FULL_VEHICLE table to the REFERENCES schema.
## Process Files:
- Splits large JSON files into smaller chunks.
- Compresses the JSON chunks into GZIP format.
- Creates a table for each JSON file in Snowflake.
- Uploads the JSON files to Snowflake stage.
- Loads data from the JSON files into the Snowflake table.
- Transforms the data into structured columns in the Snowflake table.
- Backup Tables: Backs up and renames the original tables.
## Error Handling
The script includes error handling for common issues such as network errors, file not found errors, and JSON parsing errors. It also includes a retry mechanism for uploading files to Snowflake.

## Example Usage

``` sh

python <Script_name>.py VCDB 

```
- other schema names: 'EQUIPMENT', 'QDB', 'PCDB', 'PADB'