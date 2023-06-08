from source.extract import Extract
from source.loader import Loader


def main():
    extractor = Extract('bq', dataset='dummypytharix')

    # DataSets = extractor.extract_from_db()

    # loader = Loader()
    #
    # loader.load_to_db(DataSets)

    data_set = extractor.extract_from_bq()

    print(data_set)

    extractor.close_all_connection()


if __name__ == "__main__":
    main()
