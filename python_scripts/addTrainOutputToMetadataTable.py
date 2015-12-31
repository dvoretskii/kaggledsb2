import sys
import re
import os
import dicom
import csv
from sqlalchemy import create_engine, MetaData

DB_CONNECTION_STRING = 'postgresql+psycopg2://vagrant:vagrant@127.0.0.1:54333/kaggle'
MD_TABLE_NAME = 'md'


def updateVolumesInDb(volumes, md_table, conn):
    for v in volumes:
        patient_id, vol_s, vol_d = v
        statement = md_table.update().values(vol_d=vol_d, vol_s = vol_s).where(md_table.c.path_patient_number==patient_id)
        conn.execute(statement)


def processVolumeCsvFile(csv_file_name):
    results = []
    with open(csv_file_name, 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        reader.next()  # Ignore header
        for row in reader:
            print row
            if row is not None and len(row) == 3 and row[0] is not None:
                patient_id = int(row[0])
                vol_s = float(row[1])
                vol_d = float(row[2])
                results.append((patient_id, vol_s, vol_d))
    return results


def main(csv_file_name):
    volumes = processVolumeCsvFile(csv_file_name)

    engine = create_engine(DB_CONNECTION_STRING)
    metadata = MetaData()
    metadata.reflect(engine, only=[MD_TABLE_NAME])
    md_table = metadata.tables[MD_TABLE_NAME]
    conn = engine.connect()
    updateVolumesInDb(volumes, md_table, conn)
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Need a path for train outputs csv")
