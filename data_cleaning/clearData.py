from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

class DataClear:

    # Function to remove rows where `passenger_count` is null or zero
    @staticmethod
    def filter_nonzero_passenger_count(df: DataFrame,column_name) -> DataFrame:
        """
        Removes rows where 'passenger_count' is zero or null.

        Parameters:
            dataframe (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: Filtered DataFrame.
        """
        dataframe = df.filter(col(column_name)>= 0.0)
        return dataframe


    # Function to remove rows where specified columns have zero or negative values
    @staticmethod
    def filter_positive_values(df: DataFrame, col1: str) -> DataFrame:
        """
        Filters rows where the values in the specified columns are greater than zero.

        Parameters:
            df (DataFrame): Input Spark DataFrame.
            col1 (str): Name of the first column to filter.
            col2 (str): Name of the second column to filter.

        Returns:
            DataFrame: Filtered DataFrame.
        """

        df = df.filter(col(col1) > 0)
        return df


    # Function to remove rows with all null values
    @staticmethod
    def drop_rows_with_all_nulls(df: DataFrame) -> DataFrame:
        """
        Drops rows where all columns have null values.

        Parameters:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: DataFrame with rows having all null values removed.
        """

        df = df.na.drop('all')
        return df


    # Function to fill null values with default values provided in a dictionary
    @staticmethod
    def fill_nulls_with_default(df: DataFrame, default_values: dict) -> DataFrame:
        """
        Fills null values in the DataFrame using the specified default values.

        Parameters:
            df (DataFrame): Input Spark DataFrame.
            default_values (dict): Dictionary with column names as keys and default values as values.

        Returns:
            DataFrame: DataFrame with null values replaced by default values.
        """

        df = df.na.fill(default_values)
        return df


    # Function to remove duplicate rows from the DataFrame
    @staticmethod
    def remove_duplicates(df: DataFrame) -> DataFrame:
        """
        Removes all duplicate rows from the DataFrame.

        Parameters:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: DataFrame with duplicates removed.
        """

        df = df.dropDuplicates()
        return df

    #
    # Function to remove rows with invalid payment types
    @staticmethod
    def filter_valid_payment_and_record_types(df: DataFrame,**kwargs) -> DataFrame:
        """
        Removes rows where the payment type is invalid (greater than 6 or less than or equal to 0).

        Parameters:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: Filtered DataFrame.
        """

        for item,(v1,v2) in kwargs.items():
            if not isinstance(kwargs[item] ,list) and len(kwargs[item]) > 2 :
                raise
            df = df.filter((col(item) > v1) & (col(item) <= v2))

        return df



    # Function to remove rows with invalid vendorID
    @staticmethod
    def filter_valid_correct_vendor_id(df:DataFrame):
        return  df.filter((col("VendorID") >=1) & (col("VendorID") <= 2))


    @staticmethod
    def filter_valid_dates(df: DataFrame, column_start_date: str, test_start_date: str, test_end_date: str,
                           column_end_date: str) -> DataFrame:
        """
        Filters rows based on the given date range conditions:
        - The start date (`column_start_date`) must fall within the range of `test_start_date` and `test_end_date`.
        - The end date (`column_end_date`) must also fall within the same range.

        Parameters:
            df (DataFrame): Input Spark DataFrame.
            column_start_date (str): Name of the column representing the start date.
            test_start_date (str): Start of the test date range (in 'YYYY-MM-DD' format).
            test_end_date (str): End of the test date range (in 'YYYY-MM-DD' format).
            column_end_date (str): Name of the column representing the end date.

        Returns:
            DataFrame: Filtered DataFrame.
        """

        df = df.filter(
                 (col(column_start_date) >= test_start_date) &
                (col(column_start_date) <= test_end_date) &
                (col(column_end_date) >= test_start_date) &
                (col(column_end_date) <= test_end_date))

        return df

