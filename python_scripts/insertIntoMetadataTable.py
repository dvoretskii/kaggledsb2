import sys
import re
import os
import dicom
import csv
from sqlalchemy import create_engine, MetaData

DB_CONNECTION_STRING = 'postgresql+psycopg2://vagrant:vagrant@127.0.0.1:54333/kaggle'
MD_TABLE_NAME = 'md'

AN_PATTERN = re.compile('[\W_]+')

def get_frames(root_path):
   """Get path to all the frame in view SAX and contain complete frames"""
   print("Getting frames for: " + root_path)
   ret = []
   for root, _, files in os.walk(root_path):
       if len(files) == 0 or not files[0].endswith(".dcm") or root.find("sax") == -1:
           continue
       prefix = files[0].rsplit('-', 1)[0]
       fileset = set(files)
       expected = ["%s-%04d.dcm" % (prefix, i + 1) for i in range(30)]
       if all(x in fileset for x in expected):
           ret.append([root + "/" + x for x in expected])
   # sort for reproducibility
   print("Finished getting frames")
   return sorted(ret, key = lambda x: x[0])


def process_file(cur_file, process):
    process(cur_file)


def process_all_files(frames, process):
    for paths in frames:
        for p in paths:
            process_file(p, process)


def parse_file_name(f_name):
    result = None
    ss = f_name.split('/')
    ns = len(ss)
    if ns >= 4 and ss[-1].find("dcm") > -1 and ss[-2].find("sax") > -1 and ss[-3].find("study") > -1:
        patient_number = int(ss[-4])
        sax_number = int(ss[-2].replace("sax_", ""))
        file_number = int(ss[-1].split('-')[-1].split('.')[0])
        result = (patient_number, sax_number, file_number)
    return result


def force_type(data_type, value):
    retval = None
    if value is not None:
        if data_type == "int":
            retval = int(value)
        elif data_type == "float":
            retval = float(value)
        elif data_type == "str":
            retval = str(value)
        elif data_type == "age":
            if value[-1] == 'M':
                retval = float(value.replace('M', '')) / 12.0
            else:
                retval = float(value.replace('Y', ''))
    return retval


def extract_metadata_to_db(f_name, conn, md_table, train_flag, len_dict, type_dict):
    if f_name is not None:
        f1 = dicom.read_file(f_name)
        parsed = parse_file_name(f_name)
        if parsed is not None:
            db_dict = {}
            ins = md_table.insert()
            patient_number, sax_number, file_number = parsed
            print parsed
            db_dict['is_train'] = bool(train_flag)
            db_dict['path_patient_number'] = patient_number
            db_dict['path_sax_number'] = sax_number
            db_dict['path_file_number'] = file_number
            db_dict['path'] = f_name
            for item in f1:
                name = item.description()
                name = AN_PATTERN.sub('', name)
                if name != "PixelData" and name in len_dict and name in type_dict:
                    max_seq_length = len_dict[name]
                    seq_length = item.VM
                    vals = item.value
                    data_type = type_dict[name]
                    if seq_length == 1:
                        val = force_type(data_type, vals)
                        db_dict[name] = val
                    else:
                        tmp_name = name + "Length"
                        db_dict[tmp_name] = seq_length
                        if seq_length > max_seq_length:
                            use_seq_length = max_seq_length
                        else:
                            use_seq_length = seq_length
                        for i in xrange(use_seq_length):
                            tmp_name = name + str(i)
                            val = force_type(data_type, vals[i])
                            db_dict[tmp_name] = val
            conn.execute(ins, **db_dict)

def create_process_function(conn, md_table, train_flag, len_dict, type_dict):
    return lambda f_name: extract_metadata_to_db(f_name, conn, md_table, train_flag, len_dict, type_dict)


def processMDCsvFile(csv_file_name):
    len_dict = {}
    type_dict = {}
    with open(csv_file_name, 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        reader.next()  # Ignore header
        for row in reader:
#            print row
            if row is not None and len(row) == 4 and row[0] is not None and row[1] is not None:
                base_name = row[0]
                keep = int(row[1])
                if keep:
                    seq_length = int(row[2])
                    data_type = row[3]
                    len_dict[base_name] = seq_length
                    type_dict[base_name] = data_type

    return len_dict, type_dict


def main(csv_file_name, root_path, train_flag):
    len_dict, type_dict = processMDCsvFile(csv_file_name)

    engine = create_engine(DB_CONNECTION_STRING)
    metadata = MetaData()
    metadata.reflect(engine, only=[MD_TABLE_NAME])
    md_table = metadata.tables[MD_TABLE_NAME]
    conn = engine.connect()
    frames = get_frames(root_path)
    print("Got %s frames" % len(frames))
    process_function = create_process_function(conn, md_table, train_flag, len_dict, type_dict)
    process_all_files(frames, process_function)

    conn.close()


if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    else:
        print("Need a metadata csv file name, root path and 0/1 for train/validation")
