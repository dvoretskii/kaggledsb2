```
cd python_scripts
python createMetadataTable.py ../misc_data/md_schema.csv 
python insertIntoMetadataTable.py ../misc_data/md_schema.csv ../data/train 1
python insertIntoMetadataTable.py ../misc_data/md_schema.csv ../data/validate 0
```
