from source.extract import Extract


def main():
    print('hello world')
    extractor = Extract('db', dataset='etl-trials')

    extractor.extract_from_db()

    extractor.db_connection.close()


if __name__ == "__main__":
    main()
