import happybase
connection = happybase.Connection(host='localhost', port=9090, timeout=10000)
connection.delete_table("newTable", disable=True)
