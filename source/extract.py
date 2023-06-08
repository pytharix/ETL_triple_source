import petl
import pandas
import mysql.connector
from google.cloud import bigquery
from source.config import BQ
from source.config import DB_SOURCE
from google.oauth2 import service_account
from mysql.connector import Error


class Extract:
    def __init__(self, type_extraction=None, dataset=None):
        self.__db_connection = None
        self.__bq_connection = None
        self.__data_set = dataset
        if type_extraction == "db":
            self.__connect_db()
        elif type_extraction == "bq":
            self.__connect_bq()

    def __connect_db(self):
        if self.__data_set:
            db_detail = DB_SOURCE['dev']['DATASET']
            try:
                self.__db_connection = mysql.connector.connect(
                    host=db_detail['HOSTNAME'],
                    database=self.__data_set,
                    user=db_detail['USERNAME'],
                    password=db_detail['PASSWORD']
                )

            except Error as e:
                print("Error while connecting to MySQL", e)

        else:
            print('No Dataset Selected')

    def __connect_bq(self):
        bq_detail = BQ['data']
        credentials = service_account.Credentials.from_service_account_file(
            bq_detail['location']
        )

        project_id = bq_detail['project_id']
        self.__bq_connection = bigquery.Client(credentials=credentials, project=project_id)

    def extract_from_db(self):
        # Make a cursor

        db_cursor = self.__db_connection.cursor()
        query_find_all_tables = f"""
        SELECT TABLE_NAME
          FROM information_schema.TABLES
          WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = '{self.__data_set}'
         group by 1
        """

        db_cursor.execute(query_find_all_tables)

        all_table = db_cursor.fetchall()

        data_tables = {}

        for each_table in all_table:
            current_table = each_table[0]
            sourceDataSet = petl.fromdb(self.__db_connection, f"SELECT * from {current_table}")
            # data_tables.append(sourceDataSet)
            data_tables[current_table] = sourceDataSet

        db_cursor.close()

        return data_tables

    def extract_from_bq(self):
        show_all_table = f"""
        SELECT table_name 
          FROM {self.__data_set}.INFORMATION_SCHEMA.TABLES
        """

        query_job = self.__bq_connection.query(show_all_table)

        result = list(query_job.result())

        data_tables = {}

        for each in result:
            table_name = each.get('table_name')

            table_id = f"{self.__data_set}.{table_name}"

            select_all = f"""
            SELECT * from `{table_id}`
            """
            select_all_query = self.__bq_connection.query(select_all)
            data_frame = select_all_query.result().to_dataframe()
            sourceDataSet = petl.fromdataframe(data_frame)
            data_tables[table_name] = sourceDataSet

        return data_tables

    def close_all_connection(self):
        if self.__db_connection:
            self.__db_connection.close()
