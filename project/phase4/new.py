import happybase

connection = happybase.Connection(host='localhost', port=9090)
#print(connection.tables())
'''
connection.create_table(
    'clusters',
    {'batsmanGroupId': dict(),
     'BowlerGroupId': dict(),
     'value':dict(),
    }
)
'''
table = connection.table("bowler")
row = table.row('GroupNo')
print(row)
table.put('2',{'Name': 'Sameer'})
