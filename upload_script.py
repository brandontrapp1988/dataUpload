import argparse
import json
import ssl
import zipfile
from ftplib import FTP_TLS
import os
from turtle import done
import threading
import time as tm
import re

import snowflake.connector
from datetime import datetime, timedelta

# FTP Credentials
ftp_host = '52.168.10.67'
ftp_user = 'DrivIT'
ftp_pass = ''



# Snowflake Credentials
sf_user = 'brandon'
sf_password = ''
sf_account = 'HC60396'
sf_region = 'us-east-2.aws'
sf_role = 'REFERENCE_TABLES'
sf_warehouse = 'PIPELINE'
sf_database = 'REFERENCES'
sf_database2 = 'REFERENCES_HISTORY'

# Map of keywords to their respective parameters
keyword_mapping = {
    'VCDB': ('download_vcdb/complete/json', 'AUTOCARE_VCDB', 'AUTOCARE_VCDB'),
    'EQUIPMENT': ('download_equipment/JSON', 'AUTOCARE_VCDB_EQUIPMENT', 'AUTOCARE_VCDB_EQUIPMENT'),
    'QDB': ('download_QDB/JSON', 'AUTOCARE_QDB', 'AUTOCARE_QDB'),
    'PCDB': ('download_PCDB/JSON', 'AUTOCARE_PCDB', 'AUTOCARE_PCDB'),
    'PADB': ('download_PADB/JSON', 'AUTOCARE_PADB', 'AUTOCARE_PADB'),
}

# Command-line arguments
parser = argparse.ArgumentParser(description='Process some table names.')
parser.add_argument('keyword', type=str, help='Keyword to determine parameters')
args = parser.parse_args()

# Retrieve parameters based on the keyword
if args.keyword in keyword_mapping:
    directory, source_schema, target_schema = keyword_mapping[args.keyword]
else:
    raise ValueError(f"Invalid keyword: {args.keyword}")

# Connect to Snowflake
ctx = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    region=sf_region,
    role=sf_role,
    warehouse=sf_warehouse,
    database=sf_database,
)
cur = ctx.cursor()

def create_schema_if_not_exists(schema_name):
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema {schema_name} is ready.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error occurred while creating schema {schema_name}: {e}")

def move_tables(source_schema, target_schema):
    try:
        # Ensure the source schema exists
        create_schema_if_not_exists(source_schema)

        # Switch to the target schema
        print(f"Using schema {target_schema}")
        cur.execute(f"USE SCHEMA {target_schema}")

        # Retrieve the list of tables in the target schema
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()

        # Switch to the source schema
        print(f"Using schema {source_schema}")
        cur.execute(f"USE SCHEMA {source_schema}")

        # Move each table from the target schema to the source schema
        for table in tables:
            table_name = table[1]
            # Check if the table already exists in the source schema
            cur.execute(f"SHOW TABLES LIKE '{table_name}'")
            existing_tables = cur.fetchall()

            if existing_tables:
                # Drop the existing table only if it exists in the target schema
                cur.execute(f"DROP TABLE {source_schema}.{table_name}")
                print(f"Existing table {source_schema}.{table_name} dropped.")

            # Create the table in the source schema
            cur.execute(f"""
            CREATE TABLE {source_schema}.{table_name} LIKE {target_schema}.{table_name}
            """)
            cur.execute(f"""INSERT INTO {source_schema}.{table_name} SELECT * FROM {target_schema}.{table_name}""")
            print(f"Table {target_schema}.{table_name} moved to {source_schema}.{table_name}")



    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error occurred: {e}")

def download_ftp_files(directory):
    ftps = FTP_TLS(ftp_host)
    ftps.login(user=ftp_user, passwd=ftp_pass)
    ftps.prot_p()

    downloaded_files = []

    try:
        ftps.cwd(f'/{directory}')
        print(f"Listing files in {directory}:")
        files = ftps.nlst()
        print(files)

        # Filter for ZIP files containing "current" in their name
        current_zip_files = [file for file in files if 'current' in file.lower() and file.endswith('.zip')]
        if current_zip_files:
            for file in current_zip_files:
                try:
                    local_filename = os.path.join(directory.replace('/', '_'), file)
                    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
                    with open(local_filename, 'wb') as fp:
                        ftps.retrbinary(f'RETR {file}', fp.write)
                    print(f"Successfully downloaded {file} from {directory}")
                    downloaded_files.append(local_filename)
                except Exception as e:
                    print(f"Error downloading {file} from {directory}: {e}")

    except Exception as e:
        print(f"Error accessing directory {directory}: {e}")
    finally:
        ftps.cwd('/')  # Navigate back to root directory

    ftps.quit()
    print("File download process completed.")
    return downloaded_files

def extract_zip_files(zip_files):
    extracted_files = []
    # Process zip files
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            extract_dir = os.path.splitext(zip_file)[0]
            zip_ref.extractall(extract_dir)
            extracted_files.extend([os.path.join(extract_dir, f) for f in zip_ref.namelist()])
    return extracted_files

def create_table_for_json(json_file, target_schema):
    table_name = os.path.basename(json_file).split('.')[0].upper()
    create_schema_if_not_exists(target_schema)

    # Read JSON file to infer schema
    with open(json_file) as f:
        data = json.load(f)

    # Assuming the JSON file contains an array of objects
    if data:
        first_record = data[0]
        columns = [f"{key} STRING" for key in first_record.keys()]
    else:
        columns = []

    if not columns:
        print(f"No columns found in {json_file}. Skipping table creation.")
        return None

    columns_sql = ",\n".join(columns)
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {target_schema}.{table_name} (
        {columns_sql}
    )
    """
    print(f"Generated SQL for table creation: {create_table_sql}")
    cur.execute(create_table_sql)
    print(f"Table {table_name} created in {target_schema} schema.")
    return table_name

def upload_to_snowflake(file):
    cur.execute(f"PUT file://{file} @~/staged/")
    print("File uploaded to Snowflake stage successfully.")

# Load data into tables
def load_data_into_table(file, target_schema, table_name):
    copy_command = f"""
    COPY INTO {target_schema}.{table_name}
    FROM @~/staged/{os.path.basename(file)}
    FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE'
    """
    cur.execute(copy_command)
    print(f"Data loaded into table {target_schema}.{table_name} successfully.")

# Back up and copy the tables
def backup_and_copy_tables(source_schema, target_schema):
    timestamp = datetime.now().strftime("%Y%m%d")
    new_target_schema = f"{target_schema}_{timestamp}"
    create_schema_if_not_exists(new_target_schema)

    cur.execute(f"USE SCHEMA {source_schema}")
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()

    for table in tables:
        table_name = table[1]
        backup_table_name = table_name
        cur.execute(f"CREATE TABLE {new_target_schema}.{backup_table_name} LIKE {source_schema}.{table_name}")
        cur.execute(f"INSERT INTO {new_target_schema}.{backup_table_name} SELECT * FROM {source_schema}.{table_name}")
        print(f"Table {source_schema}.{table_name} copied to {new_target_schema}.{backup_table_name}")


def create_full_vehicle_table(schema_name, combined_table_name):
    try:
        cur.execute(f"USE SCHEMA {schema_name}")

        # SQL script to create the FULL_VEHICLE table with the necessary joins
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {schema_name}.{combined_table_name} AS
        SELECT
            VV.ID AS VehicleID,
            VV.BASE_VEHICLE_ID,
            VV.SUB_MODEL_ID,
            VV.REGION_ID,
            VV.MAKE_ID,
            VV.MODEL_ID,
            VV.YEAR_ID,
            VV.MAKE,
            VV.MODEL,
            VV.YEAR,
            VV.SUB_MODEL,
            VV.REGION,
            VBC.BedConfigID,
            VBS.BodyStyleConfigID,
            VBC_VIEW.Bed_Length_ID,
            VBC_VIEW.Bed_Type_ID,
            VBC_VIEW.Bed_Length,
            VBC_VIEW.Bed_Type,
            VBS_VIEW.Body_Num_Doors_ID,
            VBS_VIEW.Body_Type_ID,
            VBS_VIEW.Body_Num_Doors,
            VBS_VIEW.Body_Type,
            VDT.DriveTypeID,
            VDT_VIEW.Drive_Type,
            VMB.MfrBodyCodeID,
            VMB_VIEW.MFR_BODY_CODE,
            VWB.WheelBaseID,
            VWB_VIEW.WHEEL_BASE,
            VWB_VIEW.WHEEL_BASE_METRIC,
            VBCF.BrakeConfigID,
            VBCF_VIEW.Front_Brake_Type_ID,
            VBCF_VIEW.Rear_Brake_Type_ID,
            VBCF_VIEW.Brake_System_ID,
            VBCF_VIEW.Brake_ABS_ID,
            VBCF_VIEW.FRONT_BRAKE_TYPE,
            VBCF_VIEW.REAR_BRAKE_TYPE,
            VBCF_VIEW.BRAKE_SYSTEM,
            VBCF_VIEW.BRAKE_ABS,
            VEC.EngineConfigID,
            VEC_VIEW.ENGINE_BASE_ID,
            VEC_VIEW.ENGINE_DESIGNATION_ID,
            VEC_VIEW.ENGINE_VIN_ID,
            VEC_VIEW.VALVES_ID,
            VEC_VIEW.FUEL_DELIVERY_CONFIG_ID,
            VEC_VIEW.ASPIRATION_ID,
            VEC_VIEW.CYLINDER_HEAD_TYPE_ID,
            VEC_VIEW.FUEL_TYPE_ID,
            VEC_VIEW.IGNITION_SYSTEM_TYPE_ID,
            VEC_VIEW.ENGINE_MFR_ID,
            VEC_VIEW.ENGINE_VERSION_ID,
            VEC_VIEW.POWER_OUTPUT_ID,
            VEC_VIEW.ENGINE_LITER,
            VEC_VIEW.ENGINE_CC,
            VEC_VIEW.ENGINE_CID,
            VEC_VIEW.ENGINE_CYLINDERS,
            VEC_VIEW.ENGINE_BLOCK_TYPE,
            VEC_VIEW.ENGINE_BORE_IN,
            VEC_VIEW.ENGINE_BORE_METRIC,
            VEC_VIEW.ENGINE_STROKE_IN,
            VEC_VIEW.ENGINE_STROKE_METRIC,
            VEC_VIEW.ENGINE_DESIGNATION,
            VEC_VIEW.ENGINE_VIN,
            VEC_VIEW.VALVES,
            VEC_VIEW.HORSE_POWER,
            VEC_VIEW.KILOWATT_POWER,
            VEC_VIEW.FUEL_DELIVERY_TYPE,
            VEC_VIEW.FUEL_DELIVERY_SUB_TYPE,
            VEC_VIEW.FUEL_SYSTEM_CONTROL_TYPE,
            VEC_VIEW.FUEL_SYSTEM_DESIGN,
            VEC_VIEW.ASPIRATION,
            VEC_VIEW.CYLINDER_HEAD_TYPE,
            VEC_VIEW.FUEL_TYPE,
            VEC_VIEW.IGNITION_SYSTEM_TYPE,
            VEC_VIEW.ENGINE_MFR,
            VEC_VIEW.ENGINE_VERSION,
            VSPC.SpringTypeConfigID,
            VSPC_VIEW.FRONT_SPRING_TYPE_ID,
            VSPC_VIEW.REAR_SPRING_TYPE_ID,
            VSPC_VIEW.FRONT_SPRING_TYPE,
            VSPC_VIEW.REAR_SPRING_TYPE,
            VSTC.SteeringConfigID,
            VSTC_VIEW.STEERING_TYPE_ID,
            VSTC_VIEW.STEERING_TYPE,
            VTC.TransmissionID,
            VTC_VIEW.TRANSMISSION_BASE_ID,
            VTC_VIEW.TRANSMISSION_TYPE_ID,
            VTC_VIEW.TRANSMISSION_CONTROL_TYPE_ID,
            VTC_VIEW.TRANSMISSION_NUM_SPEEDS_ID,
            VTC_VIEW.TRANSMISSION_MFR_CODE_ID,
            VTC_VIEW.TRANSMISSION_ELEC_CONTROLLED_ID,
            VTC_VIEW.TRANSMISSION_MFR_ID,
            VTC_VIEW.TRANSMISSION_TYPE,
            VTC_VIEW.TRANSMISSION_CONTROL_TYPE,
            VTC_VIEW.TRANSMISSION_NUM_SPEEDS,
            VTC_VIEW.TRANSMISSION_MFR_CODE,
            VTC_VIEW.TRANSMISSION_ELEC_CONTROLLED,
            VTC_VIEW.TRANSMISSION_MFR
        FROM
            REFERENCES.AUTOCARE_VCDB.VIEW_VEHICLES VV
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToBedConfig VBC
        ON
            VV.ID = VBC.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToBodyStyleConfig VBS
        ON
            VV.ID = VBS.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_BED_CONFIGS VBC_VIEW
        ON
            VBC.BedConfigID = VBC_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_BODY_STYLE_CONFIGS VBS_VIEW
        ON
            VBS.BodyStyleConfigID = VBS_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToDriveType VDT
        ON
            VV.ID = VDT.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_DRIVE_CONFIGS VDT_VIEW
        ON
            VDT.DriveTypeID = VDT_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToMfrBodyCode VMB
        ON
            VV.ID = VMB.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_MFR_BODY_CODE_CONFIGS VMB_VIEW
        ON
            VMB.MfrBodyCodeID = VMB_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToWheelBase VWB
        ON
            VV.ID = VWB.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_WHEEL_BASE_CONFIGS VWB_VIEW
        ON
            VWB.WheelBaseID = VWB_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToBrakeConfig VBCF
        ON
            VV.ID = VBCF.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_BRAKE_CONFIGS VBCF_VIEW
        ON
            VBCF.BrakeConfigID = VBCF_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToEngineConfig VEC
        ON
            VV.ID = VEC.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_ENGINE_CONFIGS VEC_VIEW
        ON
            VEC.EngineConfigID = VEC_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToSpringTypeConfig VSPC
        ON
            VV.ID = VSPC.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_SPRING_TYPE_CONFIGS VSPC_VIEW
        ON
            VSPC.SpringTypeConfigID = VSPC_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToSteeringConfig VSTC
        ON
            VV.ID = VSTC.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_STEERING_CONFIGS VSTC_VIEW
        ON
            VSTC.SteeringConfigID = VSTC_VIEW.ID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VehicleToTransmission VTC
        ON
            VV.ID = VTC.VehicleID
        LEFT JOIN
            REFERENCES.AUTOCARE_VCDB.VIEW_TRANSMISSIONS VTC_VIEW
        ON
            VTC.TransmissionID = VTC_VIEW.ID;
        """

        # Execute the SQL script
        cur.execute(create_table_sql)
        print(f"Combined table {combined_table_name} created in {schema_name} schema with the necessary joins.")

        # Move the FULL_VEHICLE table to REFERENCES schema
        move_full_vehicle_to_references(schema_name, combined_table_name)

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error occurred while creating the initial Vehicle table: {e}")

def move_full_vehicle_to_references(source_schema, table_name):
    try:
        target_schema = 'REFERENCES'

        # Ensure the target schema exists
        create_schema_if_not_exists(target_schema)

        # Switch to the source schema
        cur.execute(f"USE SCHEMA {source_schema}")

        # Move the FULL_VEHICLE table from the source schema to the target schema
        cur.execute(f"CREATE TABLE {target_schema}.{table_name} LIKE {source_schema}.{table_name}")
        cur.execute(f"INSERT INTO {target_schema}.{table_name} SELECT * FROM {source_schema}.{table_name}")
        print(f"Table {source_schema}.{table_name} moved to {target_schema}.{table_name}")

        # Drop the table from the source schema


    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error occurred while moving the FULL_VEHICLE table: {e}")

# Loading indicator function
def loading_indicator():
    while not done:
        for symbol in "|/-\\":
            if done:
                break
            print(f"\rLoading {symbol}", end="")
            tm.sleep(0.1)



# Execute the process

downloaded_files = download_ftp_files(directory)
extracted_files = extract_zip_files(downloaded_files)
for json_file in extracted_files:
    table_name = create_table_for_json(json_file, f'{sf_database2}.{target_schema}')
    if table_name:
        upload_to_snowflake(json_file)
        load_data_into_table(json_file, f'{sf_database2}.{target_schema}', table_name)
backup_and_copy_tables(f'{sf_database}.{source_schema}', f'{sf_database2}.{target_schema}')
move_tables(f'{sf_database}.{source_schema}', f'{sf_database2}.{target_schema}')


# Execute the process for VCDB
if args.keyword == 'VCDB':
    # Start the loading indicator in a separate thread
    done = False
    loading_thread = threading.Thread(target=loading_indicator)
    loading_thread.start()

    # Execute the process
    try:
        create_full_vehicle_table(f'{sf_database2}.{target_schema}', 'FULL_VEHICLE')

    except Exception as e:
        print(f"Error occurred: {e}")

    # Signal that the process is done and stop the loading indicator
    done = True
    loading_thread.join()








cur.close()
ctx.close()