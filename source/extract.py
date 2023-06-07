from source.config import DB_SOURCE
import mysql.connector
from mysql.connector import Error


class Extract:
    def __init__(self, type_extraction, dataset=None):
        self.db_connection = None
        self.data_set = dataset
        if type_extraction == "db":
            self.connect_db()

    def connect_db(self):
        if self.data_set:
            db_detail = DB_SOURCE['dev']['DATASET']
            try:
                self.db_connection = mysql.connector.connect(
                    host=db_detail['HOSTNAME'],
                    database=db_detail['DATASETNAME'],
                    user=db_detail['USERNAME'],
                    password=db_detail['PASSWORD']
                )

            except Error as e:
                print("Error while connecting to MySQL", e)

        else:
            print('No Dataset Selected')

    def extract_from_db(self):
        # Make a cursor

        db_cursor = self.db_connection.cursor()
        query_find_all_tables = f"""
        SELECT TABLE_NAME
          FROM information_schema.TABLES
          WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = '{self.data_set}'
         group by 1
        """

        db_cursor.execute(query_find_all_tables)

        all_table = db_cursor.fetchall()

        for each_table in all_table:
            print(each_table)

        db_cursor.close()
