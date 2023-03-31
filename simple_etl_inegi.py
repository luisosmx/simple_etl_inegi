!pip install unidecode
import requests
import pandas as pd
import datetime
import unidecode
from google.cloud import bigquery
import pytz
from logging import exception

URL = 'https://datos.cdmx.gob.mx/dataset/f2046fd5-51b5-4876-b008-bd65d95f9a02/resource/0e8ffe58-28bb-4dde-afcd-e5f5b4de4ccb/download/afluenciastc_simple_01_2023.csv'

def data_extraction(url: str) -> pd.DataFrame:

  # Descarga de archivo
  response = requests.get(url)
  file_path = "afluenciastc_simple_01_2023.csv"
  open(file_path, "wb").write(response.content)

  # Leer datos del archivo csv en un DataFrame de Pandas
  df = pd.read_csv("afluenciastc_simple_01_2023.csv", encoding='ISO-8859-1')
  return df 

def data_tranformation(df: pd.DataFrame) -> pd.DataFrame:

  # Renombrar columnas del DataFrame
  df = df.rename(columns={
      'fecha': 'date',
      'anio': 'year',
      'mes': 'month',
      'linea': 'line',
      'estacion': 'station',
      'afluencia': 'influx'
  })

  # Convertir nombres de líneas a minúsculas
  df['line'] = df['line'].str.lower()

  # Convertir nombres de estaciones a caracteres sin acentos
  df['station'] = df['station'].apply(lambda x: unidecode.unidecode(x))

  # Agregar fecha y hora de ingestión como una nueva columna al DataFrame
  now = datetime.datetime.now()
  ingestion_date = now.strftime('%Y-%m-%d %H:%M:%S')
  df['ingestion_date'] = ingestion_date

  # Convertir múltiples columnas a cadenas de caracteres
  df = df.applymap(str)
  df['year'] = df['year'].astype('int')
  df['influx'] = df['influx'].astype('int')
  return df

def load_data(df: pd.DataFrame) -> str:
  try:

    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "airflow-gke-381100.test_data.subway_daily_flow"


    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.STRING)
            
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        transformed_data, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    return "OK"
  except Exception as e:
    print("########################")
    print(str(e))
    print("########################")
    return "Error"

def main():
  raw_data = data_extraction(URL)
  transformed_data = data_tranformation(raw_data)
  result = load_data(transformed_data)
  print(result)
  return result

main()
