from hbase2 import HBase

def main():
    hb = HBase()
    hb.create("table1")
    hb.create("table2")
    print(hb.list())
    hb.disable("table1")
    print(hb.is_enabled("table1"))
    hb.alter("table2", col1="value1", col2="value2")
    print(hb.describe("table2"))
    hb.drop("table1")
    print(hb.list())
    hb.drop_all()
    print(hb.list())

if __name__ == "__main__":
    main()