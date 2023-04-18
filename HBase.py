import os
import time
import pandas as pd
import collections
from Table import *
from HFile import *


class HBase:
    def __init__(self):
        self.tables = {}
        self.fill_data('dataInicial/employees.csv')
        self.fill_data('dataInicial/benefits.csv')
        self.fill_data('dataInicial/performance.csv')

    def fill_data(self, file_name):
        df = pd.read_csv(file_name)
        table_name = file_name.split('/')[1].split('.')[0]
        # print(table_name)
       
        # Get the column names as a list
        columns = df.columns.tolist()
        # print(columns)
        # Extract the column families by splitting each column name on the colon and taking the first element
        column_families = collections.OrderedDict()
        column_qualifiers = collections.OrderedDict()
        for col in columns[1:]:
            cf = col.split(":")[0]
            if cf not in column_families:
                column_families[cf] = True
            cq = col.split(":")[1]
            if cq not in column_qualifiers:
                column_qualifiers[cq] = True

        column_families = list(column_families.keys())
        column_qualifiers = list(column_qualifiers.keys())

        # print(column_families)  # Output: ['personal', 'professional']
        # print(column_qualifiers)  # Output: ['name', 'age', 'salary', 'designation']
        
        command = f"create {table_name}, {column_families[0]}, {column_families[1]}"
        self.create(command)
        
        for i in range(len(df)):
            row = df.iloc[i, :]
            row_key = df.iloc[i, 0]
            value1 = df.iloc[i, 1]
            value2 = df.iloc[i, 2]
            value3 = df.iloc[i, 3]
            value4 = df.iloc[i, 4]
            self.put(table_name, row_key, column_families[0], column_qualifiers[0], value1)
            self.put(table_name, row_key, column_families[0], column_qualifiers[1], value2)
            self.put(table_name, row_key, column_families[1], column_qualifiers[2], value3)
            self.put(table_name, row_key, column_families[1], column_qualifiers[3], value4)
        

    def create(self, command):
        table_name = command.split()[1].replace(",", "")
        if table_name not in self.tables:
            newTable = Table(table_name)
            newTable.columnFamilies =  [word.replace(",", "") for word in command.split()[2:]]
            # print(newTabl e.columnFamilies)

            self.tables[newTable.name] = newTable
            # print(hbase.tables)

        else:
            print(f"Table '{command.split()[1]}' already exists")

    def put(self, table_name, row_key, column_family, column_qualifier, value):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
        # convert the input data to the desired format
        table_data = {
            row_key: {
                column_family: {
                    column_qualifier: value
                }
            }
        }
        # call the modified put() method that accepts the table_data parameter
        self.put_table_data(table_name, table_data)
        hfile = HFile(table_name)
        hfile.save_table_to_hfile(table_name, self.tables[table_name].rows)

    def put_table_data(self, table_name, table_data):
        for row_key, columns_data in table_data.items():
            if row_key not in self.tables[table_name].rows:
                self.tables[table_name].rows[row_key] = {}
            for column_family, columns in columns_data.items():
                if column_family not in self.tables[table_name].columnFamilies:
                    print(f"Column family '{column_family}' does not exist in table '{table_name}'")
                    return
                for column_qualifier, value in columns.items():
                    if isinstance(value, dict):
                        for sub_column_qualifier, sub_value in value.items():
                            timestamp = int(time.time())
                            if sub_column_qualifier not in self.tables[table_name].rows[row_key].get(column_qualifier, {}):
                                self.tables[table_name].rows[row_key][column_qualifier][sub_column_qualifier] = {}
                            self.tables[table_name].rows[row_key][column_qualifier][sub_column_qualifier] = (sub_value, timestamp)
                    else:
                        timestamp = int(time.time())
                        if column_family not in self.tables[table_name].rows[row_key]:
                            self.tables[table_name].rows[row_key][column_family] = {}
                        if column_qualifier not in self.tables[table_name].rows[row_key][column_family]:
                            self.tables[table_name].rows[row_key][column_family][column_qualifier] = {}
                        self.tables[table_name].rows[row_key][column_family][column_qualifier] = (value, timestamp)


    def list_tables(self):
        return list(self.tables.keys())

    def disable(self, table_name):
        if table_name in self.tables:
            self.tables[table_name].enabled = False
            print(f"Table '{table_name}' disabled")
        else:
            print(f"Table '{table_name}' does not exist")
    
    def enable(self, table_name):
        if table_name in self.tables:
            self.tables[table_name].enabled = True
            print(f"Table '{table_name}' enabled")
        else:
            print(f"Table '{table_name}' does not exist")
    
    def is_enabled(self, table_name):
        if table_name in self.tables:
            print(self.tables[table_name].enabled)
        else:
            print(f"Table '{table_name}' does not exist")
    
    def alter(self, table_name, option):
        if table_name in self.tables:
            option = option.replace("{", "").replace("}", "")
            if option.split()[0] == "NAME":
                new_name = option.split()[2]
                self.tables[table_name].name = new_name
                self.tables[new_name] = self.tables[table_name]
                del self.tables[table_name]
                print(f"Table '{table_name}' renamed to '{new_name}'")
                oldPath = f"hfiles/tabla_{table_name}.csv"
                newPath = f"hfiles/tabla_{new_name}.csv"
                os.rename(oldPath, newPath)
                # print(self.tables)
                # print(self.tables[new_name].name)
            elif option.split()[0] == "ADD":
                new_column_family = option.split()[1]
                self.tables[table_name].columnFamilies.append(new_column_family)
                print(f"Column family '{new_column_family}' added to table '{table_name}'")
                # print(self.tables[table_name].columnFamilies)
            elif option.split()[0] == "DELETE":
                column_family = option.split()[1]
                self.tables[table_name].columnFamilies.remove(column_family)
                print(f"Column family '{column_family}' deleted from table '{table_name}'")
                self.delete_cf(table_name, column_family)
                # print(self.tables[table_name].columnFamilies)
        else:
            print(f"Table '{table_name}' does not exist")

    def drop(self, table_name):
        if table_name in self.tables:
            del self.tables[table_name]
            hfile_path = f"hfiles/tabla_{table_name}.csv"
            os.remove(hfile_path)
            print(f"Table '{table_name}' dropped")
        else:
            print(f"Table '{table_name}' does not exist")

    def drop_all(self):
        for table in self.tables:
            hfile_path = f"hfiles/tabla_{table}.csv"
            try:
                os.remove(hfile_path)
            except:
                pass
        self.tables = {}
        print("All tables dropped")

    def describe(self, table_name):
        if table_name in self.tables:
            print(f"Table name: '{table_name}'")
            print(f"Table '{table_name}' is_enabled:  {self.tables[table_name].enabled}")
            print(f"Column families: {self.tables[table_name].columnFamilies}")
        else:
            print(f"Table '{table_name}' does not exist")


    def scan(self, table_name):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
        df = pd.read_csv(f"hfiles/tabla_{table_name}.csv")
        print(df)

    def get(self, table_name, row_key, columnF = None, columnQ = None):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
        # print(f"Table name: '{table_name}'")
        # print(f"Row key: '{row_key}'")
        # print(f"Column family: '{columnF}'")
        # print(f"Column qualifier: '{columnQ}'")
        df = pd.read_csv(f"hfiles/tabla_{table_name}.csv")
        if columnF is None and columnQ is None:
            print(df[df["row key"] == row_key])
        elif columnF is not None and columnQ is None:
            print(df[(df['row key'] == row_key) & (df['cf:col'].str.startswith(f'{columnF}:'))])
        elif columnF is not None and columnQ is not None:
            print(df[(df["row key"] == row_key) & (df["cf:col"] == columnF+":"+columnQ)])

    def delete_cf(self, table_name, columnF):
        rows = self.tables[table_name].rows
        for key, value in rows.items():
            if columnF in value:
                del value[columnF]
        hfile = HFile(table_name)
        hfile.save_table_to_hfile(table_name, rows)


    def delete(self, table_name, row_key, columnF = None, columnQ = None):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
        if row_key not in self.tables[table_name].rows:
            print(f"Row '{row_key}' does not exist in table '{table_name}'")
            return

        if columnF is None and columnQ is None:
            # Remove the entire row
            del self.tables[table_name].rows[row_key]
            print(f"Row '{row_key}' deleted from table '{table_name}'")
        elif columnF is not None and columnQ is None:
            # Remove all columns of the specified column family
            if columnF in self.tables[table_name].rows[row_key]:
                del self.tables[table_name].rows[row_key][columnF]
                print(f"All columns of column family '{columnF}' deleted from row '{row_key}' in table '{table_name}'")
            else:
                print(f"Column family '{columnF}' does not exist in row '{row_key}' in table '{table_name}'")
        elif columnF is not None and columnQ is not None:
            # Remove the specified column
            if columnF in self.tables[table_name].rows[row_key] and columnQ in self.tables[table_name].rows[row_key][columnF]:
                del self.tables[table_name].rows[row_key][columnF][columnQ]
                print(f"Column '{columnQ}' of column family '{columnF}' deleted from row '{row_key}' in table '{table_name}'")
            else:
                print(f"Column '{columnQ}' of column family '{columnF}' does not exist in row '{row_key}' in table '{table_name}'")
        else:
            print("Invalid input")
        hfile = HFile(table_name)
        hfile.save_table_to_hfile(table_name, self.tables[table_name].rows)
    
    def delete_all(self, table_name, row_key, columnF = None, columnQ = None):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
        if row_key not in self.tables[table_name].rows:
            print(f"Row '{row_key}' does not exist in table '{table_name}'")
            return

        if columnF is None and columnQ is None:
            # Remove the entire row
            del self.tables[table_name].rows[row_key]
            print(f"Row '{row_key}' deleted from table '{table_name}'")
        elif columnF is not None and columnQ is None:
            # Remove all columns of the specified column family
            if columnF in self.tables[table_name].rows[row_key]:
                del self.tables[table_name].rows[row_key][columnF]
                print(f"All columns of column family '{columnF}' deleted from row '{row_key}' in table '{table_name}'")
            else:
                print(f"Column family '{columnF}' does not exist in row '{row_key}' in table '{table_name}'")
        elif columnF is not None and columnQ is not None:
            # Remove the specified column
            if columnF in self.tables[table_name].rows[row_key] and columnQ in self.tables[table_name].rows[row_key][columnF]:
                del self.tables[table_name].rows[row_key][columnF][columnQ]
                print(f"Column '{columnQ}' of column family '{columnF}' deleted from row '{row_key}' in table '{table_name}'")
            else:
                print(f"Column '{columnQ}' of column family '{columnF}' does not exist in row '{row_key}' in table '{table_name}'")
        else:
            print("Invalid input")
        hfile = HFile(table_name)
        hfile.save_table_to_hfile(table_name, self.tables[table_name].rows)
        
    def count(self, table_name, startRow = None, endRow = None):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
        df = pd.read_csv('hfiles/tabla_'+table_name + ".csv")
        if startRow is None and endRow is None:
            print(f"Number of rows in table '{table_name}': {len(df)}")
        elif startRow is not None and endRow is None:
            count = 0
            for i in range(len(df)):
                if df.iloc[i,0] == startRow:
                    count += 1
                
            print(f"Number of rows in table '{table_name}' with row_key '{startRow}': {count}")
        elif startRow is not None and endRow is not None:
            count = 0
            for i in range(len(df)):
               if df.iloc[i,0] == startRow:
                   for j in range(i,len(df)):
                        if df.iloc[j,0] == endRow:
                           break
                        else:
                           count += 1
                   break
            print(f"Number of rows in table '{table_name}' from row '{startRow}' to row '{endRow}': {count}")
        else:
            print("Invalid input")

    def truncate(self, table_name):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
        self.tables[table_name].rows = {}
        print(f"Disabling table '{table_name}'")
        self.tables[table_name].enabled = False
        print(f"Table '{table_name}' disabled")
        print("Deleting table file")
        print(f"Table '{table_name}' truncated")
        print(f"Enabling table '{table_name}'")
        self.tables[table_name].enabled = True
        print(f"Table '{table_name}' enabled")
        hfile = HFile(table_name)
        hfile.save_table_to_hfile(table_name, self.tables[table_name].rows)
   