import jaydebeapi
from jdbc_variables import *


def connect_to_db():
    try:

       # Établir la connexion
        connection = jaydebeapi.connect(
            "org.postgresql.Driver",
            jdbc_url,
            [db_properties["user"], db_properties["password"]],
            jdbc_driver_path  # Chemin vers le fichier .jar du driver JDBC
        )

        connection.jconn.setAutoCommit(False)
        return connection

    except Exception as ex:
        print(f"Erreur lors de la connexion à la base de données : {str(ex)}")
        return None


def execute_sql_file(filename, cursor):
    # Lire le fichier SQL et exécuter les commandes
    with open(filename, 'r') as sql_file:
        sql_commands = sql_file.read()
        # Diviser les commandes par point-virgule pour les exécuter individuellement
        commands = sql_commands.split(';')
        for command in commands:
            command = command.strip()
            if command:  # Éviter les commandes vides
                try:
                    cursor.execute(command)
                except Exception as e:
                    print(f"Erreur lors de l'exécution de la commande : {command}")
                    print(f"Détails de l'erreur : {e}")





connection = connect_to_db()




"""
try:
    # Connexion à la base de données
    connection = connect_to_db()
    if connection:
        cursor = connection.cursor()

        # Exécution du fichier schema.sql pour créer les tables
        execute_sql_file('Ressource/schema.sql', cursor)
        connection.commit()  # Valider les changements pour la création des tables
        print("Les tables ont été créées avec succès.")

        # Exécution du fichier data.sql pour insérer les données
        execute_sql_file('Ressource/data.sql', cursor)
        connection.commit()  # Valider les changements pour l'insertion des données
        print("Les données ont été insérées avec succès.")

except Exception as e:
    print("Erreur lors de l'exécution des fichiers SQL :", e)
finally:
    if connection:
        cursor.close()
        connection.close()
"""
