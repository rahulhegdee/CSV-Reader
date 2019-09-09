import csv
import numpy as numpy
from messytables import CSVTableSet, type_guess, types_processor, headers_guess, headers_processor, offset_processor

tinyint_min = 0
tinyint_max = 255
smallint_min = -32768
smallint_max = 32767
int_min = -2147483648
int_max = 2147483647
bigint_min = -9223372036854775808
bigint_max = 9223372036854775807

real_min = -3.4E+38
real_max = 3.4E+38
float_min = -1.7E+308
float_max = 1.7E+308


def get_row_count(file_name):  # counts the rows in the csv file

    try:
        # opens the csv file being used as a reading file
        with open(file_name, 'r') as csv_file:

            # sets the file to the reader variable
            reader = csv.reader(csv_file)
            row_number = 0

            # each row in the csv file is a list. The row variable counts for each list (or row)
            for row in reader:
                # adds 1 to the row number each time a new row is counted by the for loop
                row_number += 1

            # closes the csv file
            csv_file.close()

            # returns the number of rows (not including the header)
            return row_number - 1

    except Exception:
        # makes sure that if the row count fails, the rest of the functions still work
        print("Row Count Unavailable")


def get_column_count(file_name):  # counts the columns in the csv file

    try:
        with open(file_name, 'r') as csv_file:
            reader = csv.reader(csv_file)

            # goes to the first row of the csv file
            first_row = next(reader)
            csv_file.close()

            # counts each item in a single row list (which is the number of columns)
            return len(first_row)

    except Exception:
        # makes sure that if the col count fails, rest of the functions still works
        print("Column Count Unavailable")


def get_column_names(file_name):  # gives the name of the columns in the csv file in an array

    try:
        with open(file_name, 'r') as csv_file:
            reader = csv.reader(csv_file)

            # the first row is the header, next goes to the next row which is the first one here
            header = next(reader)

            # array is not native to python, this converts the list to an array
            header_array = numpy.array(header)
            csv_file.close()

            # returns the array of column names
            return header_array

    except Exception:
        # makes sure that if the col name fails, rest of the functions still works
        print("Column Names Unavailable")


def get_column_pytypes(file_name):  # gives the python data types of each column

    try:
        # The messytables module helps us get accurate python data types through the following:

        # opens the csv file in read binary mode
        file_table = open(file_name, "rb")

        # Next, we load the file into a table set and assign that table set under the "table_types" variable:
        table_types = CSVTableSet(file_table)

        # Since a table set is a set of multiple tables, we create our table:
        column_types = table_types.tables[0]

        # We save the first row (headers) under the offset and headers variables
        offset, headers = headers_guess(column_types.sample)

        # We tell the table that the first row is under the headers variable
        column_types.register_processor(headers_processor(headers))

        # Then we tell the table that the rest of the content is under the offset variable
        # We add one to begin with the content and not the header
        column_types.register_processor(offset_processor(offset + 1))

        # Next, we guess the data types of each column in the table
        # strict=True means that a type will still be guessed even if parsing fails for a cell in the column
        types = type_guess(column_types.sample, strict=True)

        # We apply the data types to each column of the table
        column_types.register_processor(types_processor(types))

        # stores the number of headers (or columns essentially)
        header_length = len(headers)

        datatype_list = []  # creates the data type list

        # the for loop will be used to iterate through the first column (0) to the last one (header_length)
        for current_col in range(0, header_length):
            # We convert the current type of 'messytables.type' to a string so that we can write to it
            types[current_col] = str(types[current_col])

            # messytables uses the type Decimal instead of the python type float:
            if types[current_col] == 'Decimal':  # if the data type of a column is called "Decimal"
                types[current_col] = "Float"  # change the name of that data type to be called "Float"

            # we use a string to indicate which header contains which type
            type_assignment = f"{headers[current_col]} : {types[current_col]}"

            # appends the type_assignment for each column into the list
            datatype_list.append(type_assignment)

            # converts the list to an array
        datatype_array = numpy.array(datatype_list)
        file_table.close()

        # returns the array with all the data types
        return datatype_array

    except Exception:
        # makes sure that if the types fails, rest of the functions still works
        print("Python Datatypes Unavailable")


def get_sql_types(file_name):
    try:
        with open(file_name, 'r') as csv_file:

            # The following block of code uses the messytables module and is explained in the get_column_types function:
            file_table = open(file_name, "rb")
            table_types = CSVTableSet(file_table)
            column_types = table_types.tables[0]
            offset, headers = headers_guess(column_types.sample)
            column_types.register_processor(headers_processor(headers))
            column_types.register_processor(offset_processor(offset + 1))
            types = type_guess(column_types.sample, strict=True)
            column_types.register_processor(types_processor(types))

            # creates a lambda that checks if the string being passed is ascii or unicode
            # by checking if the string is the same as it would be if it was encoded
            # since ascii is 1 byte and Unicode is 2 bytes, this checks if they are the same length
            is_ascii = lambda s: len(s) == len(s.encode())

            # gives the number of columns
            column_length = len(headers)

            # creates list
            sql_list = []

            # iterates through the first to last column
            for current_col in range(0, column_length):
                # We convert the current type of 'messytables.type' to a string so that we can write to it
                types[current_col] = str(types[current_col])

                if types[current_col] == 'Bool':  # if the data type is a Bool
                    types[current_col] = 'Bit'  # change name to "Bit"

                if types[current_col] == 'String':  # if the data type is a String
                    reader = csv.reader(csv_file)

                    # variable that will be used to prevent the header from being a part of the column
                    header_check = False

                    # variable that will be used to switch between VarChar and NVarChar
                    nvarchar_check = False

                    # variable that will be used to find the longest string within each column
                    char_length = 0

                    # restarts the file from the very start for each new string column
                    csv_file.seek(0)

                    # for each row in the column
                    for row in reader:
                        # if not the header
                        if header_check:
                            # if past longest string is less than length of current
                            if char_length < len(row[current_col]):
                                # change the longest length to the current string length
                                char_length = len(row[current_col])

                            # if the current string is in ascii
                            if is_ascii(row[current_col]):
                                # if no previous strings were unicode in current column
                                if not nvarchar_check:
                                    # change name to "VarChar(max length)"
                                    types[current_col] = f'VarChar({char_length})'

                                # if any previous string was unicode in current column
                                elif nvarchar_check:
                                    # change name to 'NVarChar(max length)"
                                    types[current_col] = f'NVarChar({char_length})'

                            # if the current string is in unicode
                            elif not is_ascii(row[current_col]):
                                # change name to 'NCharVar(max length)"
                                types[current_col] = f'NVarChar({char_length})'

                                # make sure the column is identified as NVarChar
                                nvarchar_check = True

                        header_check = True  # checks that the header has been passed

                # if the data type is an Integer
                if types[current_col] == 'Integer':
                    reader = csv.reader(csv_file)

                    # variable that will be used to prevent the header from being a part of the column
                    header_check = False

                    # variable that will be used to switch between each sql int type
                    int_check = 0

                    # restarts the file from the very start for each new string column
                    csv_file.seek(0)
                    for row in reader:

                        # if not the header
                        if header_check:
                            # We convert the current type of 'messytables.type' to an int so that we can write to it
                            row[current_col] = int(row[current_col])

                            # if the current int is between the minimum and maximum value of tinyint
                            if tinyint_min <= row[current_col] <= tinyint_max:
                                # if no other numbers in the column go past the same values
                                if int_check == 0:
                                    # change name to TinyInt
                                    types[current_col] = 'TinyInt'

                            if smallint_min <= row[current_col] < tinyint_min \
                                    or tinyint_max < row[current_col] <= smallint_max:
                                # if no other numbers in the column go past the same values
                                if int_check <= 1:
                                    # change name to 'SmallInt'
                                    types[current_col] = 'SmallInt'
                                    int_check = 1

                            if int_min <= row[current_col] < smallint_min \
                                    or smallint_max < row[current_col] <= int_max:
                                # if no other numbers in the column go past the same values
                                if int_check <= 2:
                                    # change name to 'Int'
                                    types[current_col] = 'Int'
                                    int_check = 2

                            if bigint_min <= row[current_col] <= int_min \
                                    or int_max < row[current_col] <= bigint_max:
                                # change name to 'BigInt'
                                types[current_col] = 'BigInt'

                                # remain BigInt even if future numbers in column are smaller
                                int_check = 3

                        header_check = True  # iterates through the column

                # if the data type is 'Decimal'
                if types[current_col] == 'Decimal':
                    reader = csv.reader(csv_file)

                    # variable that will be used to prevent the header from being a part of the column
                    header_check = False

                    # variable that will be used to switch between Real and Float
                    float_check = False

                    # restarts the file from the very start for each new string column
                    csv_file.seek(0)
                    for row in reader:
                        # if not the header
                        if header_check:
                            # We convert the current type of 'messytables.type' to a string so that we can write to it
                            row[current_col] = float(row[current_col])

                            # if current float in column is between these values
                            if real_min <= row[current_col] <= real_max:
                                # if no previous float in column has exceeded these boundaries
                                if not float_check:
                                    # Change name to 'Real'
                                    types[current_col] = 'Real'

                            if float_min <= row[current_col] < real_min or real_max < row[current_col] <= float_max:
                                # change name to 'Float'
                                types[current_col] = 'Float'
                                float_check = True

                        header_check = True  # checks if header has been reached yet

                # assigns each sql type to each header in a string
                sql_names = f"{headers[current_col]} : {types[current_col]}"

                # appends the header + type string into list
                sql_list.append(sql_names)

            # converts list into an array
            sql_array = numpy.array(sql_list)
            csv_file.close()
            file_table.close()

            # returns the array of sql types
            return sql_array

    except Exception:
        # makes sure that if the sql types fails, rest of the functions still works
        print("SQL Types Unavailable")
