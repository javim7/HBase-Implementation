class Table:
    def __init__(self, name):
        self.name = name
        self.enabled = True
        self.columnFamilies = []
        self.rows = {}
    