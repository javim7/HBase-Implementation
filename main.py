from HBase import *
from HFile import *
from Table import *

contador = 1

hbase = HBase()


while True:
    if len(str(contador)) == 1:
        command = input(f"\nhabase(main):00{contador}:0> ") # Prompt the user for input with a "> " symbol
        contador += 1
    else:
        command = input(f"\nhabase(main):0{contador}:0> ")
        contador += 1


    first_word = command.split()[0]
    if first_word == "create":
       hbase.create(command)

    elif command == "list":
        tables = hbase.list_tables()
        for table in tables:
            print(table)

    elif first_word == "disable":     
        table_name = command.split()[1]
        hbase.disable(table_name)

    elif first_word == "is_enabled":
        table_name = command.split()[1]
        hbase.is_enabled(table_name)

    elif first_word == "alter":
        table_name = command.split()[1].replace(",", "")
        option = command.split(",")[1]
        hbase.alter(table_name, option)

    elif first_word == "drop":
        table_name = command.split()[1]
        hbase.drop(table_name)

    elif command == "drop_all":
        hbase.drop_all()

    elif first_word == "describe":
        table_name = command.split()[1]
        hbase.describe(table_name)

    elif first_word == "put":
        parts = command.split(",")
        table_name = parts[0].strip().split()[1]
        row_key = parts[1].strip()
        column_family, column_qualifier = parts[2].strip().split(":")
        value = parts[3].strip()

        # print(table_name, row_key, column_family, column_qualifier, value)
        hbase.put(table_name, row_key, column_family, column_qualifier, value)
        # print(hbase.tables[table_name].rows)

    elif command == "quit":
        break # Exit the loop if the user types "quit"

    else:
        print(f"Command '{command}' not recognized") # Display an error message for unrecognized commands