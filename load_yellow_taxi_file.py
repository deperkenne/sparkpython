
import os
import requests

# telechargement des fichier parquets en local

Base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

year = [2023,2024]
month = ["01","02"]

def createe_change_directory(file):
    if not os.path.isdir(file):
        os.mkdir(file)
    os.chdir(file)

def download_file():
    for y in year:
        for m in month:
            file_name = f"yellow_tripdata_{y}-{m}.parquet"
            url = f"{Base_url}/{file_name}"
            file_path = f"./{file_name}"

            try:
                # Téléchargement du fichier
                response = requests.get(url, stream=True)
                response.raise_for_status()  # Vérifier si la requête a échoué
            except requests.exceptions.RequestException as e:
                print(f"Erreur lors du téléchargement de {file_name}: {e}")
                continue

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    print(f"Téléchargement terminé pour : {file_name}")



createe_change_directory("File_parquet")
download_file()
