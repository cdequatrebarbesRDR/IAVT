#!/usr/bin/venv/python
# AVENANT_DB
import os
import shutil
from glob import glob
import re
import csv
from pymongo import MongoClient
from models import Contrat
from models import Document
from models import Companie

class DB:
    def __init__(self):
        #self.client = AsyncMongoClient("mongodb://localhost:27017/")
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["AVENANTS"]
    
    def init(self):
        self.populate_documents_from_fs()
        self.populate_contrats_from_csv()
        self.group_police_number_by_cie()
        self.group_police_edition_by_cie()
        print(self.db.contrats.count_documents({}), "contrats")
        print(self.db.documents.count_documents({}), "documents")
        return 
    def reset(self):
        self.db["documents"].drop()
        self.db["contrats"].drop()
    def populate_documents_from_fs(self, input_dir="avenants_input", output_dir="AVENANTS_ALL"):
        self.db["documents"].drop()
        for filepath in glob(os.path.join(input_dir, '**', '*.pdf'), recursive=True):
            rel_filepath = filepath.replace(input_dir, "")
            chunks_path = rel_filepath.split("/")
            cie_dir = chunks_path[1]
            filename = re.sub(r"[\s+|-]", "_", chunks_path[-1])
            new_filepath = os.path.join(output_dir, str(cie_dir)+"_"+filename)
            shutil.copyfile(filepath, new_filepath)
            d = Document(new_filepath)
            self.db["documents"].insert_one(d.__dict__)

    def populate_contrats_from_csv(self, csv_filename="TK2501333.csv", delimiter=";"):
        self.db["contrats"].drop()
        with open(csv_filename, 'r', encoding='latin-1', errors='ignore') as csvfile:
            spamreader = csv.DictReader(csvfile, delimiter=delimiter)
            for i, row in enumerate(spamreader):
                if None in row.keys():
                    row_values = list(row.values())
                    if isinstance(row_values[-1], list):
                        tmp_last = row_values[-1][0]
                        row_values[-1] = tmp_last 
                    tmp_cell = " ".join(row_values[6:8])
                    row_values.pop(7)
                    row_values[6] = tmp_cell
                    
                    corr_row = dict(zip(list(row.keys()), row_values))
                    corr_row["POLNUM"] = corr_row["POLNUM"].strip()
                    c = Contrat(corr_row)
                    self.db["contrats"].insert_one(c.__dict__)
                else:
                    try:
                        row["POLNUM"] = row["POLNUM"].strip()
                        c = Contrat(row)
                        self.db["contrats"].insert_one(c.__dict__)
                    except Exception as e:
                        print(i+1, e)
                        continue

    def get_distinct_police_editions(self):
        return self.db.contrats.distinct("poledi")      
        
    def get_distinct_police_numbers(self):
        return self.db.contrats.distinct("polnum")
    
    def group_police_number_by_cie(self):
        self.db.contrats.aggregate([
            { "$group" : { "_id" : "$cienom", "police_numbers": { "$push": "$polinum" } } },
            { "$out" : "cie_police_number" }
        ])
        return self.db['cie_police_number']
    
    def group_police_edition_by_cie(self):
        self.db.contrats.aggregate([
            { "$group" : { "_id" : "$cienom", "police_editions": { "$push": "$poledi" } } },
            { "$out" : "cie_poledi" }
        ])
        return self.db.cie_police_edition
    
    def group_contrat_by_cie(self):
        self.db.contrats.aggregate([
            { "$group" : { "_id" : "$cienom", "contrats": { "$push": "$$ROOT" } } },
            { "$out" : "cie_contrats" }
        ])

if __name__ == "__main__":
    db = DB()
    db.init()