import os
import tarfile
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ContainerClient
import gzip
import shutil

def compress_and_upload_files():
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = "landing"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Calculer la date du jour précédent
    yesterday = datetime.now() - timedelta(1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    tar_name = f"/tmp/{yesterday_str}.tar"
    gzip_name = f"/tmp/{yesterday_str}.tar.gz"

    # Sélectionner tous les fichiers du dossier du jour précédent
    blobs = container_client.list_blobs(name_starts_with=yesterday_str)

    # Créer un fichier tar avec tous les fichiers sélectionnés
    with tarfile.open(tar_name, "w") as tar:
        for blob in blobs:
            blob_client = container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob().readall()
            # Écrire le contenu du blob dans un fichier temporaire
            temp_file_name = f"/tmp/{blob.name.split('/')[-1]}"  # Extraire le nom du fichier depuis le chemin complet
            with open(temp_file_name, "wb") as temp_file:
                temp_file.write(blob_data)
            # Ajouter le fichier temporaire au tar
            tar.add(temp_file_name, arcname=blob.name.split('/')[-1])
            # Supprimer le fichier temporaire
            os.remove(temp_file_name)

    # Compresser le fichier .tar en .gzip
    with open(tar_name, 'rb') as f_in:
        with gzip.open(gzip_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Supprimer le fichier .tar après la compression
    os.remove(tar_name)

    # Uploader le fichier .gzip dans le dossier d'archive
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f'archive/{gzip_name.split("/")[-1]}')
    with open(gzip_name, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    # Supprimer le fichier .gzip local après l'upload
    os.remove(gzip_name)
