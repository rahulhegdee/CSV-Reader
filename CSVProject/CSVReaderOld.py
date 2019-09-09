import csv
import numpy as np
from messytables import CSVTableSet, type_guess, types_processor, headers_guess, headers_processor, offset_processor


file = "6.csv"
x = 0
try:
    with open (file, 'r') as csv_file:

        reader = csv.reader(csv_file)
        for row in reader:
            # if you are trying to delete the column that just numbers the content, remove the # of this:
            # del (row[0])
            if x == 0:
                cN = row
            if x == 1:
                test = row[1]
            # print(row)
            x += 1
        print("Rows: ", x - 1)  # prints the number of rows (minus the header)
        print("Columns: ", len(row))  # prints the number of columns

        cN_array = np.array(cN)
        print("Header: ", cN_array)

        ft = open(file, "rb")
        tableTypes = CSVTableSet(ft)
        columnTypes = tableTypes.tables[0]
        offset, headers = headers_guess(columnTypes.sample)
        columnTypes.register_processor(headers_processor(headers))
        columnTypes.register_processor(offset_processor(offset + 1))
        types = type_guess(columnTypes.sample, strict=True)
        columnTypes.register_processor(types_processor(types))

        h_length = len(headers)
        h_var = 0
        dt_array = []
        for each_type in range(0, h_length):
            types[h_var] = str(types[h_var])
            if types[h_var] == 'Decimal':
                types[h_var] = "Float"
            randVar = f"{headers[h_var]} : {types[h_var]}"
            dt_array.append(randVar)
            h_var += 1
        print("Datatypes: ", dt_array)  #

except Exception:
    print("File Unavailable")


csv_file.close()
