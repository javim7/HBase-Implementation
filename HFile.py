import os
import csv
import operator

class HFile:
    def __init__(self, table_name):
        self.table_name = table_name
        self.hfile_path = f"tabla_{self.table_name}.txt"
    
    # Función para guardar una tabla en un archivo de HFile
    def save_table_to_hfile(self, table_name, table):
        hfile_path = f"tabla_{table_name}.txt"
        with open(hfile_path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(["row key", "cf:col", "timestamp", "value"]) # Adding header row
            for row_key, row in table.items():
                for cf, columns in row.items():
                    for cq, (value, ts) in columns.items():
                        writer.writerow([row_key.strip(), f"{cf}:{cq}".strip(), str(ts).strip(), value.strip()])
