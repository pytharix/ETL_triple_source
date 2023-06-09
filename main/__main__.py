from source.extract import Extract
from source.loader import Loader
import petl


def main():
    extractor = Extract('api')

    data_set = extractor.extract_from_api()

    # print(data_set)

    loader = Loader(type_target='BQ')

    loader.load_to_bq(
        creat_new=True,
        data_set_id='target_dataset_230609',
        data_sets=data_set
    )

    extractor.close_all_connection()


if __name__ == "__main__":
    main()
