from HBase import HBase
from HFile import HFile

contador = 1

hbase = HBase()
table_name = 'ex1'
hbase = HBase()
hbase.create(table_name)
hbase.put(table_name, 'cf1', 'cq1', 'value1')
hbase.put(table_name, 'cf1', 'cq2', 'value2')
hbase.put(table_name, 'cf1', 'cq1', 'value3')
hbase.put(table_name, 'cf2', 'cq1', 'value4')

hbase.create('ex2')
hbase.put('ex2', 'cf1', 'cq1', 'value1')
hbase.put('ex2', 'cf1', 'cq2', 'value2')

for k,v in hbase.tables.items():
    print(k,v)

# create an HFile from the table data
hf = HFile(table_name)
hf.save_table_to_hfile(table_name, hbase.tables[table_name])

while True:
    if len(str(contador)) == 1:
        command = input(f"\nhabase(main):00{contador}:0> ") # Prompt the user for input with a "> " symbol
        contador += 1
    else:
        command = input(f"\nhabase(main):0{contador}:0> ")
        contador += 1


    first_word = command.split()[0]
    if command == "list":
        tables = hbase.list()
        for table in tables:
            print(table)
    elif command == "quit":
        break # Exit the loop if the user types "quit"
    else:
        print(f"Command '{command}' not recognized") # Display an error message for unrecognized commands