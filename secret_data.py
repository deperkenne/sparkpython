import configparser

# Charger le fichier de configuration
config = configparser.ConfigParser()
config.read('../../../../Credentielle/credentielle.properties')

# Extraire les informations de connexion
jdbc_url = config['DEFAULT']['jdbc_url']
jdbc_driver_path = config['DEFAULT']['jdbc_driver_path']
db_properties = {
    "user": config['DEFAULT']['user'],
    "password": config['DEFAULT']['password'],
    "driver": config['DEFAULT']['driver']
}