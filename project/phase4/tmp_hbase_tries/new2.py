import happybase

def main(table_name):
    connection = happybase.Connection(host='localhost', port=9090, timeout=10000)

    try:
        print('Creating the {} table.'.format(table_name))
        column_family_name = 'cf1'
        connection.create_table(
            table_name,
            {
                column_family_name: dict()
            })

        print('Writing some greetings to the table.')
        table = connection.table(table_name)
        column_name = '{fam}:greeting'.format(fam=column_family_name)
        greetings = [
            'Hello World!',
            'Hello Cloud Bigtable!',
            'Hello HappyBase!',
        ]

        for i, value in enumerate(greetings):
            row_key = 'greeting{}'.format(i)
            table.put(row_key, {column_name: value})

        print('Getting a single greeting by row key.')
        key = 'greeting0'.encode('utf-8')
        row = table.row(key)
        print('\t{}: {}'.format(key, row[column_name.encode('utf-8')]))

        for key, row in table.scan():
            print('\t{}: {}'.format(key, row[column_name.encode('utf-8')]))

        print('Deleting the {} table.'.format(table_name))
        connection.delete_table(table_name, disable=True)

    finally:
        connection.close()


if __name__ == '__main__':
    main("newTable")
