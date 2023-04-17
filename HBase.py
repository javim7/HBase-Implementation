import os
import time
from Table import *
from HFile import *


class HBase:
    def __init__(self):
        self.tables = {}

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
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist")
            return
        if not self.tables[table_name].enabled:
            print(f"Table '{table_name}' is disabled")
            return
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
                # print(self.tables[table_name].columnFamilies)
        else:
            print(f"Table '{table_name}' does not exist")

    def drop(self, table_name):
        if table_name in self.tables:
            del self.tables[table_name]
            hfile_path = f"hfiles/tabla_{table_name}.txt"
            os.remove(hfile_path)
            print(f"Table '{table_name}' dropped")
        else:
            print(f"Table '{table_name}' does not exist")

    def drop_all(self):
        for table in self.tables:
            hfile_path = f"hfiles/tabla_{table}.txt"
            os.remove(hfile_path)
        self.tables = {}
        print("All tables dropped")

    def describe(self, table_name):
        if table_name in self.tables:
            print(f"Table name: '{table_name}'")
            print(f"Table '{table_name}' is_enabled:  {self.tables[table_name].enabled}")
            print(f"Column families: {self.tables[table_name].columnFamilies}")
        else:
            print(f"Table '{table_name}' does not exist")


    def get(self, table_name, row_key, column_family=None, column_qualifier=None, timestamp=None):
        if table_name not in self.tables:
            raise ValueError("La tabla '{}' no existe.".format(table_name))

        if row_key not in self.tables[table_name]:
            return None

        if column_family is None:
            return self.tables[table_name][row_key]

        if column_family not in self.tables[table_name][row_key]:
            return None

        if column_qualifier is None:
            return self.tables[table_name][row_key][column_family]

        if column_qualifier not in self.tables[table_name][row_key][column_family]:
            return None

        if timestamp is None:
            return self.tables[table_name][row_key][column_family][column_qualifier]

        value, ts = self.tables[table_name][row_key][column_family][column_qualifier]
        if ts <= timestamp:
            return value
        else:
            return None

    def delete(self, table_name, row_key, column_family=None, column_qualifier=None, timestamp=None):
        if table_name not in self.tables:
            raise ValueError("La tabla '{}' no existe.".format(table_name))

        if row_key not in self.tables[table_name]:
            return

        if column_family is None:
            del self.tables[table_name][row_key]
            return

        if column_family not in self.tables[table_name][row_key]:
            return

        if column_qualifier is None:
            del self.tables[table_name][row_key][column_family]
            return

        if column_qualifier not in self.tables[table_name][row_key][column_family]:
            return

        if timestamp is None:
            del self.tables[table_name][row_key][column_family][column_qualifier]
        else:
            value, ts = self.tables[table_name][row_key][column_family][column_qualifier]
            if ts <= timestamp:
                del self.tables[table_name][row_key][column_family][column_qualifier]