import csv
import sys
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean

DB_CONNECTION_STRING = 'postgresql+psycopg2://vagrant:vagrant@127.0.0.1:54333/kaggle'
MD_TABLE_NAME = 'md'

def getDataTypeClass(data_type):
    dtc = None
    if data_type == "int":
        dtc = Integer
    elif data_type == "float":
        dtc = Float
    elif data_type == "str":
        dtc = String(100)
    elif data_type == "age":
        dtc = Integer
    elif data_type == "bool":
        dtc = Boolean
    return dtc


def processCsvFile(csv_file_name):
    columns = []
    columns.append(Column('md_id', Integer, primary_key=True))
    columns.append(Column('path', String, ))
    columns.append(Column('path_patient_number', Integer, ))
    columns.append(Column('path_sax_number', Integer, ))
    columns.append(Column('path_file_number', Integer, ))
    columns.append(Column('is_train', Boolean, ))
    with open(csv_file_name, 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        reader.next()  # Ignore header
        for row in reader:
            print row
            if row is not None and len(row) == 4 and row[0] is not None and row[1] is not None:
                base_name = row[0]
                keep = int(row[1])
                if keep:
                    seq_length = int(row[2])
                    data_type = row[3]
                    dtc = getDataTypeClass(data_type)
                    if dtc is not None:
                        if seq_length == 1:
                            columns.append(Column(base_name, dtc))
                        else:
                            full_name = base_name + "Length"
                            columns.append(Column(full_name, Integer))
                            for i in xrange(seq_length):
                                full_name = base_name + str(i)
                                columns.append(Column(full_name, dtc))


    columns.append(Column('vol_d', Float))
    columns.append(Column('vol_s', Float))
    return columns

def createMDTable(columns, engine):
    metadata = MetaData()
    md_table = Table(MD_TABLE_NAME, metadata, *columns)
    metadata.create_all(engine)

def main(csv_file_name):
    engine = create_engine(DB_CONNECTION_STRING)
#    connection = engine.connect()
    columns = processCsvFile(csv_file_name)
    createMDTable(columns, engine)
#    connection.close()


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Need a csv file name")
