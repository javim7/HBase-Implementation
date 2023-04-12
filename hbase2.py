class HBase:
    def __init__(self):
        self.tables = {}

    def create(self, table_name):
        if table_name in self.tables:
            print(f"Table {table_name} already exists")
        else:
            self.tables[table_name] = {"enabled": True}
            print(f"Table {table_name} created")

    def list(self):
        return list(self.tables.keys())

    def disable(self, table_name):
        if table_name in self.tables:
            self.tables[table_name]["enabled"] = False
            print(f"Table {table_name} disabled")
        else:
            print(f"Table {table_name} does not exist")

    def is_enabled(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name]["enabled"]
        else:
            print(f"Table {table_name} does not exist")
            return False

    def alter(self, table_name, **kwargs):
        if table_name in self.tables:
            self.tables[table_name].update(kwargs)
            print(f"Table {table_name} altered")
        else:
            print(f"Table {table_name} does not exist")

    def drop(self, table_name):
        if table_name in self.tables:
            del self.tables[table_name]
            print(f"Table {table_name} dropped")
        else:
            print(f"Table {table_name} does not exist")

    def drop_all(self):
        self.tables = {}
        print("All tables dropped")

    def describe(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name]
        else:
            print(f"Table {table_name} does not exist")