from source.extract import Extract
from source.loader import Loader
from source.transform import Transform
import petl


def main():
    # data_set = 'dummypytharix'
    # extractor = Extract('BQ', dataset=data_set)
    # print('Current Data Set id: ', data_set)
    #
    # data_set = 'dummypytharix'
    extractor = Extract('FLAT')

    data_sets = extractor.extract_from_flat()
    # data_ss = data_sets['customer']
    # liss = list(petl.values(data_ss, 'Fax'))
    # for i in liss:
    #     print(i, type(i))

    loader = Loader('BQ')

    # data_sets = extractor.extract()

    loader.load_to_bq(
        creat_new=False,
        data_set_id="customer_gender_rev",
        data_sets=data_sets
    )
    print(data_sets)

    # transformer = Transform(datasets=data_sets, preview=False)
    #
    # transformer.optimization(type_of='Gender_maker')

    # transformer.drop_col('source')

    # transformer.print_data()

    extractor.close_all_connection()


if __name__ == "__main__":
    main()
