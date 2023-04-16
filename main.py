from HBase import HBase
from HFile import HFile

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

# print the table
# print(table)
