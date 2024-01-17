import gc
import os
import sys
from minio import Minio
from minio.error import MinioException
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine


def download_from_minio(minio_client: Minio, bucket: str, object_name: str) -> BytesIO:
    """Télécharge un objet depuis Minio et renvoie le contenu sous forme de BytesIO."""
    try:
        response = minio_client.get_object(bucket, object_name)
        content_bytes = BytesIO(response.read())
        print(f"Downloaded {object_name} from Minio")
        return content_bytes
    except MinioException as err:
        print(f"Error downloading {object_name} from Minio: {err}")
        raise

def write_data_postgres(dataframe: pd.DataFrame, name: str) -> bool: 
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }
    
    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful! Processing parquet file", name)
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
            print(f"Success to download the file {name} to the nyc_raw", name)

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:
    # folder_path: str = r'..\..\data\raw'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    minio_client = Minio(
        "http://127.0.0.1:9000/",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    bucket = "datalake"
    prefix = "raw/"

    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.datalake') and os.path.isfile(os.path.join(folder_path, f))]
    dataframes = []  # Liste pour stocker les DataFrames

    for parquet_file in parquet_files:
        # Utilisation de la fonction download_from_minio pour récupérer les données
        content_bytes = download_from_minio(minio_client, bucket, prefix + parquet_file)
        
        # Utilisation de pandas pour lire les données depuis BytesIO
        parquet_df = pd.read_parquet(content_bytes, engine='pyarrow')
        
        clean_column_name(parquet_df)
        dataframes.append(parquet_df)  # Stocke le DataFrame dans la liste

    # Exemple : Concaténer tous les DataFrames dans un seul DataFrame
    concatenated_df = pd.concat(dataframes, ignore_index=True)

    # Charger le DataFrame concaténé dans la base de données
    if not write_data_postgres(concatenated_df):
        print("Échec lors de l'écriture dans la base de données.")

    # Libérer la mémoire en supprimant les DataFrames
    del dataframes
    del concatenated_df
    gc.collect()


if __name__ == '__main__':
    sys.exit(main())
    
