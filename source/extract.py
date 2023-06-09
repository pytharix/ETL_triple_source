import re
import petl
import json
import pandas
import requests
import mysql.connector
from google.cloud import bigquery
from mysql.connector import Error
from google.oauth2 import service_account
from source.config import DB_SOURCE, API_SC
from source.config import BQ_SC as BQ_Sources


class Extract:
    def __init__(self, type_extraction=None, dataset=None):
        #  Connection (type of Extraction)
        self.__db_connection = None
        self.__bq_connection = None
        self.__api_url = None

        #  Type of Extraction to make the connection
        if type_extraction:
            self.__type_extraction = type_extraction.lower()
        else:
            self.__type_extraction = None

        #  Connecting DB
        if self.__type_extraction == "db":
            self.__connect_db()

        #  Connection BQ
        elif self.__type_extraction == "bq":
            self.__connect_bq()

        #  "Connecting" API - since api don't need a connection to specific tools
        elif self.__type_extraction == 'api':
            self.__api_url = API_SC['data']['url']

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
        bq_detail = BQ_Sources['data']
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

    def extract_from_api(self, table_name=None):
        if not table_name:
            table_name=MyClass.create_table_name(API_SC['data']['url'])
        data = requests.get(self.__api_url).text

        jsonLoc = API_SC['data']['jsonLoc']

        with open(jsonLoc, 'w') as f:
            f.write(data)

        # header_table = list(data[0].keys())
        header_table = ['postId', 'id', 'name', 'email', 'body']

        table1 = petl.fromjson(jsonLoc, header=header_table)

        return {table_name: table1}

    def close_all_connection(self):
        if self.__db_connection:
            self.__db_connection.close()


class MyClass:
    @staticmethod
    def create_table_name(string_):
        string_list = string_.split('/')[3:]
        warm_name = ''
        for each in string_list:
            warm_name += each
        luke_table_name = re.findall(r'[a-zA-Z]+', warm_name)
        table_name = ''

        for each in luke_table_name:
            table_name += each

        return table_name
