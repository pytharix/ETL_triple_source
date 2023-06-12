import petl
import numpy


class Transform:
    def __init__(self, datasets: dict, preview=True):
        self.__datasets = datasets
        if preview:
            self.__data_information()

    def __data_information(self):
        data_sets = self.__datasets

        for each_table_name, each_data in data_sets.items():
            print("Table name: ", each_table_name)

            headers = each_data.header()
            many_rows = len(each_data)
            for each_column in headers:
                each_unique = petl.valuecounter(each_data, each_column)
                print("   Column: ", each_column)
                print('      Total Rows   : ', many_rows)
                data_types = list(petl.typeset(each_data, each_column))
                print("      Type         : ", data_types)

                values = list(petl.values(each_data, each_column))

                print("      Data         : ", values[:2])

                many_unique = len(each_unique.values())

                print('      Unique Data  : ', many_unique)

                print('      Missing Value: ', each_unique.get(None))

                if len(data_types):
                    if data_types[0] == "int":
                        print('      Max Value    : ', max(values))
                        print('      Min Value    : ', min(values))
                        print('      Average Value: ', numpy.average(values))

                print('')

    def __drop_col_1(self, column_names_p, each_table_name, each_data):
        data_column_names = list(petl.header(each_data))
        if all(isinstance(x, int) for x in column_names_p):
            for each_index in column_names_p:
                try:
                    data_column_names.pop(each_index)
                except:
                    print(f"Index {each_index} doesn't exist")
        else:
            for each_column in column_names_p:
                try:
                    data_column_names.remove(each_column)
                except:
                    print(f"Column {each_column} doesn't exist")

        self.__datasets[each_table_name] = petl.cut(each_data, data_column_names)

    def drop_col(self, column_names, table_name=None):
        if type(column_names) != list:
            column_names_p = [column_names]
        else:
            column_names_p = column_names

        if not table_name:
            for each_table_name, each_data in self.__datasets.items():
                self.__drop_col_1(column_names_p, each_table_name, each_data)

        else:
            if type(table_name) != list:
                table_delete = [table_name]
            else:
                table_delete = table_name

            for each_table in table_delete:
                try:
                    each_table_name = each_table
                    each_data = self.__datasets[each_table_name]
                    self.__drop_col_1(column_names_p, each_table_name, each_data)
                except:
                    print(f"table {each_table} doesn't exist")

    def sort_col(self, sort_array=None, table_name=None):
        pass

    def optimization(self, type_of=None):
        asssss = None

        source_api = f"https://api.genderize.io?name={asssss}"

        if type_of == "Gender_maker":
            for each_table_name, each_data in self.__datasets.items():
                name_source = list(petl.values(each_data, 'contact_name'))
                print(name_source)



    def print_data(self):
        for each_table_name, each_data in self.__datasets.items():
            print(each_table_name)
            print(each_data)


class MyClass:
    @staticmethod
    def unique(list1):
        unique_list = []

        for x in list1:
            if x not in unique_list:
                unique_list.append(x)

        return unique_list
