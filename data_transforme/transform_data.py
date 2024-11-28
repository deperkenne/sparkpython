import string
from datetime import datetime
from time import strftime
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, when, lit, unix_timestamp, typeof
from pyspark.sql.types import IntegerType
import validation
from validation import DataFrameNotFoundException, ColumnNotExistException


class TransformData:

    @staticmethod
    def transform_float_to_integer(df: DataFrame,columnName:str) -> DataFrame:
        """
        Transforme la colonne 'passenger_count' d'un type float en int.
        Args:
            df (DataFrame): Le DataFrame Spark à modifier.
            df (columnName): la column a convertir en integer
        Returns:
            DataFrame: Le DataFrame avec columnName converti en int.
            :param df:
            :param columnName:

        """

        if  columnName not in  df.columns:
            raise validation.ColumnNotExistException("this columnName not exist")
        return df.withColumn(columnName, col(columnName).cast("int"))

    @staticmethod
    def transform_long_to_integer(df:DataFrame,columnName:str):
        if columnName not in df.columns:
            raise validation.ColumnNotExistException("this columnName not exist")
        return df.withColumn(columnName, col(columnName).cast("integer"))

    @staticmethod
    def drop_multiple_columns(df: DataFrame, *args: str) -> DataFrame:
        """
        Supprime plusieurs colonnes du DataFrame.
        Args:
            df (DataFrame): Le DataFrame Spark à modifier.
            args (str): Les noms des colonnes à supprimer.
        Returns:
            DataFrame: Le DataFrame avec les colonnes spécifiées supprimées.
        Raises:
            ValueError: Si une colonne n'est pas une chaîne.
        """
        if not all(isinstance(item, str) for item in args):
            raise validation.ColumnNotExistException("this column not exist.")
        for item in args:
            df = df.drop(item)
        return df

    @staticmethod
    def rename_one_column(df: DataFrame, old_col_name: str, new_col_name: str) -> DataFrame:
        """
        Renomme une colonne spécifique du DataFrame.
        Args:
            df (DataFrame): Le DataFrame Spark à modifier.
            old_col_name (str): Le nom actuel de la colonne.
            new_col_name (str): Le nouveau nom de la colonne.
        Returns:
            DataFrame: Le DataFrame avec la colonne renommée.
        Raises:
            ValueError: Si les noms de colonnes ne sont pas des chaînes ou si la colonne n'existe pas.
        """
        if old_col_name not in df.columns:
            raise validation.ColumnNotExistException(f"La colonne '{old_col_name}' n'existe pas.")
        return df.withColumnRenamed(old_col_name, new_col_name)

    @staticmethod
    def update_one_column(df: DataFrame, col_name: str, col_value, set_value) -> DataFrame:
        """
        Met à jour une colonne basée sur une condition.
        Args:
            df (DataFrame): Le DataFrame Spark à modifier.
            col_name (str): Le nom de la colonne à modifier.
            col_value: La valeur conditionnelle.
            set_value: La nouvelle valeur à attribuer si la condition est remplie.
        Returns:
            DataFrame: Le DataFrame avec la colonne mise à jour.
        """
        return df.withColumn(col_name, when(col(col_name) == col_value, set_value).otherwise(col(col_name)))

    def update_multiple_col(self ,df,**kwargs):
        if not isinstance(df,DataFrame):
            raise validation.DataFrameNotFoundException("df to update not exist")
        for var_col, [old_value,new_value] in kwargs.items():
            df = df.withColumn(var_col,when(col(var_col) == old_value,new_value).otherwise(col(var_col)))
        return df

    @staticmethod
    def add_column(df:DataFrame, new_column_name:str,old_colName:str) -> DataFrame:
        """
        Ajoute une nouvelle colonne 'col_name' avec une valeur calculée.
        Args:
            df (DataFrame): Le DataFrame Spark à modifier.
            new_column_name (str): Le nom de la nouvelle colonne.
            old_column_name (str): Le nom de la column qui servira a inserer les valeurs dans la nouvelle column.
        Returns:
            DataFrame: Le DataFrame avec la nouvelle colonne ajoutée.
            :param new_column_name:
            :param old_colName:
        """
        return df.withColumn(new_column_name, col(old_colName) * 1000)


    @staticmethod
    def select_column(df:DataFrame,*args):
        """
            Sélectionne un ensemble prédéfini de colonnes pour la première partition.
            Args:
                df (DataFrame): Le DataFrame Spark à modifier.
            Returns:
                DataFrame: Le DataFrame contenant uniquement les colonnes sélectionnées.

            """




        # Boucle pour générer dynamiquement la sélection
        selected_columns = [df[col] for col in args]

        # Utiliser select() avec la liste des colonnes
        df_selected = df.select(*selected_columns)


        return df_selected


    @staticmethod
    def rename_multiple_columns(df: DataFrame, **kwargs) -> DataFrame:
        """
        Renomme plusieurs colonnes dans le DataFrame.
        Args:
            df (DataFrame): Le DataFrame Spark à modifier.
            kwargs: Paires clé-valeur représentant old_col_name -> new_col_name.
        Returns:
            DataFrame: Le DataFrame avec les colonnes renommées.
        Raises:
            ValueError: Si une colonne à renommer n'existe pas.
        """
        for old_col_name, new_col_name in kwargs.items():
            if old_col_name not in df.columns:
                raise validation.ColumnNotExistException(f"La colonne '{old_col_name}' n'existe pas.")
            df = df.withColumnRenamed(old_col_name, new_col_name)
        return df


    @staticmethod
    def add_time_in_minutes(df: DataFrame, new_column_name: str, start_date: str, end_date: str) -> DataFrame:
        """
        Adds a new column to the DataFrame that represents the difference in minutes between two datetime columns.

        :param df: The input Spark DataFrame.
        :param new_column_name: The name of the new column to be added.
        :param start_date: The name of the column containing the start date.
        :param end_date: The name of the column containing the end date.
        :return: A new DataFrame with the additional column.
        """
        # Calculate the difference in seconds and convert to minutes
        df = df.withColumn(
            new_column_name,
            (unix_timestamp(col(end_date)) - unix_timestamp(col(start_date))) / 60
        )
        return df