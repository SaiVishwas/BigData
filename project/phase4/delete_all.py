import happybase
connection = happybase.Connection(host='localhost', port=9090)
print("Following tables will be delated :",connection.tables())
for table_name in connection.tables():
    connection.delete_table(table_name, disable=True)
