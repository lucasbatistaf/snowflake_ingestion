import os, glob
import pandas as pd
import snowflake.connector

from dotenv import load_dotenv

load_dotenv()

# Defining variables
folder_path = os.getenv('DATA_FOLDER_LOCATION')
snowflake_stage = os.getenv('SNOWFLAKE_STAGE')

# Connection to Snowflake
conn = snowflake.connector.connect(
    user = os.getenv('SNOWFLAKE_USERNAME'),
    password = os.getenv('SNOWFLAKE_PASSWORD'),
    account = os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
    database = os.getenv('SNOWFLAKE_DATABASE'),
    schema = os.getenv('SNOWFLAKE_SCHEMA')
)

cursor = conn.cursor()

# Defining the columns for the new csv
columns = ['DTOBITO',
           'DTNASC',
           'SEXO',
           'RACACOR',
           'ESC',
           'CODMUNRES',
           'CODMUNOCOR',
           'CAUSABAS']


def process_csv():
    for dirpath, dirnames, filenames in os.walk(folder_path):
        dataframes = []

        for filename in filenames:
            if filename.endswith('.csv'):
                file_path = os.path.join(dirpath, filename)
                try:
                    df = pd.read_csv(file_path)
                    # Select only the desired columns
                    df_selected = df[columns]
                    dataframes.append(df_selected)

                    concatenated_df = pd.concat(dataframes, ignore_index=True)
                    concatenated_df.to_csv(f'{dirpath}.csv', sep=';', encoding='utf-8', index=False)
                    print(f"Processed: {file_path}")
                except Exception as e:
                    print(f"Error processing {filename}: {e}")

def send_to_snoflake():
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    for filename in csv_files:
        local_file_path = os.path.join(folder_path, filename)
        print(local_file_path)
        put_command = f"PUT 'file://{local_file_path}' @{snowflake_stage} AUTO_COMPRESS=FALSE;"
        cursor.execute(put_command)
        print(f"Uploaded {filename} to stage.")

    cursor.close()
    conn.close()

process_csv()
send_to_snoflake()