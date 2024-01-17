from minio import Minio
import urllib.request
import pandas as pd
import sys
import requests
from io import BytesIO
import os # Ajout de la bibliothèque os pour la gestion des fichiers

def main():
    grab_data()
    return 0    

def grab_data() -> None:
    annee = 2023
    dernier_mois = 8
    donnees_parquet = {} 
    for mois in range(1, dernier_mois + 1):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{annee}-{mois:02d}.parquet'
        print(f"Téléchargement du fichier de : {annee}-{mois:02d}")
        # Télécharger le fichier .parquet à partir de l'URL
        reponse = requests.get(url)
        reponse.raise_for_status()
        # Charger les données du fichier .parquet dans un DataFrame pandas
        fichier_parquet = pd.read_parquet(BytesIO(reponse.content))
        # Construire le chemin pour sauvegarder le fichier
        chemin_fichier = f"C:/Users/jorda/ATL-Datamart-1/src/data/raw/yellow_tripdata_{annee}-{mois:02d}.parquet"
        
        # Sauvegarder le fichier dans le dossier "../../data/raw"
        fichier_parquet.to_parquet(chemin_fichier)
        
        # Afficher un message de confirmation
        print(f"Fichier {annee}-{mois:02d} sauvegardé dans {chemin_fichier}")
    
        # Stocker les données dans le dictionnaire
        nom_fichier = f"Fichier de {annee}-{mois:02d}"
        donnees_parquet[nom_fichier] = fichier_parquet
    write_data_minio(donnees_parquet)
            
def write_data_minio(data):
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "datalake"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    for nom_fichier, dataframe_parquet in data.items():
        # Convertir le DataFrame Pandas en fichier parquet binaire
        parquet_bytes = dataframe_parquet.to_parquet()
        # Ajouter l'année-mois au nom de l'objet dans Minio
        object_name = f"{nom_fichier}.parquet"
        
        # Mettre l'objet dans le compartiment (bucket) Minio
        client.put_object(bucket, object_name, BytesIO(parquet_bytes), len(parquet_bytes))
        print(f"Objet {object_name} ajouté dans Minio.")

if __name__ == '__main__':
    sys.exit(main())
