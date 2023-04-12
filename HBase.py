import happybase

connection = happybase.Connection('localhost', port=9090)


def create_table(table_name, column_families):
    connection.create_table(table_name, column_families)


def list_tables():
    return connection.tables()


def disable_table(table_name):
    connection.disable_table(table_name)


def is_table_enabled(table_name):
    return connection.is_table_enabled(table_name)


def alter_table(table_name, column_families):
    connection.disable_table(table_name)
    connection.alter_table(table_name, column_families)
    connection.enable_table(table_name)


def drop_table(table_name):
    connection.delete_table(table_name, disable=True)


def drop_all_tables():
    for table in connection.tables():
        connection.delete_table(table, disable=True)


def describe_table(table_name):
    return connection.table(table_name).schema


# Ejemplo de uso
table_name = 'test_table'
column_families = {
    'cf1': dict(),
    'cf2': dict(),
}
create_table(table_name, column_families)
print(list_tables())
# alter_table(table_name, {'cf3': dict()})
# print(describe_table(table_name))
# disable_table(table_name)
# print(is_table_enabled(table_name))
# drop_table(table_name)
# print(list_tables())
# drop_all_tables()
# print(list_tables())
