from Db_connect import *

from Ressource.file_path_variable import *




def execute_sql_file(filename, cursor):
    # Lire le fichier SQL et exécuter les commandes
    with open(filename, 'r') as sql_file:
      if filename.startswith("data"):
         pass
      else:
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


try:
    # Connexion à la base de données
    connection = connect_to_db()
    if connection:
        cursor = connection.cursor()

        # Exécution du fichier schema.sql pour créer les tables
        execute_sql_file(yellow_taxi_schema_partition, cursor)
        connection.commit()  # Valider les changements pour la création des tables
        print("Les tables ont été créées avec succès.")
        connection.commit()  # Valider les changements pour l'insertion des données
        print("Les données ont été insérées avec succès.")

except Exception as e:
    print("Erreur lors de l'exécution des fichiers SQL :", e)
finally:
    if connection:
        cursor.close()
        connection.close()