from source.config import DB_ME
from mysql.connector import Error
import pymysql
import petl


class Loader:
    def __init__(self):
        self.__db_connection = None
        self.__connect_db()

    def __connect_db(self):
        db_detail = DB_ME['dev']['DATASET']
        try:
            self.__db_connection = pymysql.connect(
                host=db_detail['HOSTNAME'],
                database=db_detail['DATASETNAME'],
                user=db_detail['USERNAME'],
                password=db_detail['PASSWORD']
            )

        except Error as e:
            print("Error while connecting to MySQL", e)

    def load_to_db(self, data_sets: dict):
        if self.__db_connection:
            curs = self.__db_connection.cursor()
            curs.execute('SET SQL_MODE=ANSI_QUOTES')
            for each_table, each_data in data_sets.items():
                curs.execute(f"DROP TABLE if EXISTS {each_table}")
                petl.todb(each_data, self.__db_connection, each_table, create=True)
