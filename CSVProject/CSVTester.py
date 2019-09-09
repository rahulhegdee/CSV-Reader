from CSVReader import get_row_count, get_column_count, get_column_names, get_column_pytypes, get_sql_types

file = input("File Name: ")

print("Rows: ", get_row_count(file))  # Counts all the rows except the Header row

print("Columns: ", get_column_count(file))  # Counts all the columns as long as there are commas dividing them

print("Headers: ", get_column_names(file))  # As long as the first row is the header

print("Datatypes: ", get_column_pytypes(file))  # Assumes only 0/1 in a column means column is a Bool

print("SQL Types: ", get_sql_types(file))

