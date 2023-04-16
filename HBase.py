import time


class HBase:
    def __init__(self):
        self.tables = {}

    def create(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = {}

    def put(self, table_name, column_family, column_qualifier, value, timestamp=None):
        if table_name not in self.tables:
            raise ValueError("La tabla '{}' no existe.".format(table_name))

        num_rows = len(self.tables[table_name])
        row_key = str(num_rows + 1) # Generate numeric row key based on number of existing rows

        if row_key not in self.tables[table_name]:
            self.tables[table_name][row_key] = {}

        if column_family not in self.tables[table_name][row_key]:
            self.tables[table_name][row_key][column_family] = {}

        if timestamp is None:
            timestamp = int(time.time())

        self.tables[table_name][row_key][column_family][column_qualifier] = (value, timestamp)

        if timestamp is None:
            timestamp = int(time.time())

        self.tables[table_name][row_key][column_family][column_qualifier] = (value, timestamp)

    def list(self):
        return list(self.tables.keys())

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