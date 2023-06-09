from source.config import DB_ME
from mysql.connector import Error
from google.oauth2 import service_account
from source.config import BQ_TG as BQ_target
from google.cloud import bigquery
import pymysql
import petl


class Loader:
    def __init__(self, type_target=None):
        self.__db_connection = None
        self.__bq_detail = None

        if type_target:
            self.__type_target = type_target.lower()
        else:
            self.__type_target = None

        if self.__type_target == 'db':
            self.__connect_db()

        elif self.__type_target == 'bq':
            self.__connect_bq()

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

    def __connect_bq(self):
        self.__bq_detail = BQ_target['data']
        credentials = service_account.Credentials.from_service_account_file(
            self.__bq_detail['location']
        )

        project_id = self.__bq_detail['project_id']
        self.__bq_connection = bigquery.Client(credentials=credentials, project=project_id)

    def load_to_db(self, data_sets: dict):
        if self.__db_connection:
            curs = self.__db_connection.cursor()
            curs.execute('SET SQL_MODE=ANSI_QUOTES')
            for each_table, each_data in data_sets.items():
                curs.execute(f"DROP TABLE if EXISTS {each_table}")
                petl.todb(each_data, self.__db_connection, each_table, create=True)

    def load_to_bq(self, creat_new=False, data_set_id=None, data_sets: dict = None):
        datasets = self.__bq_connection.list_datasets()
        list_of_dataset = []
        for each_dataset in datasets:
            dataset = ("{}".format(each_dataset.dataset_id))
            list_of_dataset.append(dataset)

        dataset_id_registered = data_set_id in list_of_dataset

        if creat_new:
            if dataset_id_registered:
                print('Create new failed')
                return  # Exit
            print('Creating new')
            MyClass.create_data_set(
                client=self.__bq_connection,
                dataset_id=data_set_id
            )

        else:
            if not dataset_id_registered:
                print('Override failed')
                return  # Exit
            print('Overriding')

        # Insert Dataset

        for each_table, each_data in data_sets.items():
            converted_data_df = petl.todataframe(each_data)

            project_id = self.__bq_detail['project_id']
            table_id = each_table

            converted_data_df.to_gbq(
                destination_table=f'{project_id}.{data_set_id}.{table_id}',
                project_id=project_id,
                if_exists='replace'
            )

            print(f"Table {each_table} inserted.")

        print("Process Load Success")


class MyClass:
    @staticmethod
    def create_data_set(client, dataset_id):
        location = "US"

        dataset_ref = client.dataset(dataset_id)

        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        return
