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

class DB:
    def __init__(self, DB_NAME="AVENANTS"):
        #self.client = AsyncMongoClient("mongodb://localhost:27017/")
        self.name = DB_NAME
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[DB_NAME]
    
    def init(self):
        self.populate_contrats_from_csv()
        self.populate_documents_from_fs()
        print(self.db.contrats.count_documents({}), " contrats")
        print(self.db.documents.count_documents({}), " documents")
        return 
    
    def preprocess(self):
        if self.db.contrats.count_documents({}) == 0 or self.db.contrats.count_documents({}): 
            self.init()
        self.group_police_number_by_cie()
        self.group_police_edition_by_cie()
        self.group_contrat_by_cie()
        self.group_document_by_cie()

    def reset(self):
        self.db["documents"].drop()
        self.db["contrats"].drop()
    
    def search_doc_in_contracts(self, d):
        d.found = False
        if d.ref is None:
            return d.found
        query = {"cie.name": d.cie.name,"$or": [{"poledi":{"$regex":d.ref.strip()}},{"polnum":{"$regex":d.ref.strip()}}]}
        nb_match = self.db["contrats"].count_documents(query)
        if nb_match == 0 and not hasattr(d, "original_ref"):
            if d.cie.name == "GROUPAMA":
                d.original_ref = d.ref
                d.ref = d.ref.split("/")[1]
                return self.search_doc_in_contracts(self, d)
            return d.found
        d.found = True
        first_match = self.db["contrats"].find_one(query)
        d.matches = {"count": nb_match, "poledi_num": self.db["contrats"].find(query).distinct('poledi')}
        d.poledi = first_match["poledi"]
        d.polnum = first_match["polnum"]
        d.entrai = first_match["entrai"]
        d.numper = first_match["numper"]
        return d.found
    
    def store_doc(self, d, reset=False):
        if reset:
            self.db["documents"].insert_one(d.__document__())
            return d
        record = self.db.documents.find_one({"filename": d.filename}, {"_id":1})
        self.db["documents"].update_one({"_id": record["_id"]}, {"$set":d.__document__()})
        return d
    
    def populate_documents_from_fs(self, input_dir="./avenants_input/", reset = False):
        print(f"INSERTING DOCS from {input_dir} INTO DB")
        if reset:
            self.db.documents.drop()
        for filepath in glob(os.path.join(input_dir, '**', '*.pdf'), recursive=True):
            #if still ALLIANZ in documents
            if not "AZ" in filepath:
                d = Document(filepath)
                self.search_doc_in_contracts(d)
                self.store_doc(d)
                
                


    def populate_contrats_from_csv(self, csv_filename="TK2501333.csv", delimiter=";"):
        self.db["contrats"].drop()
        #self.db.contrats.createIndex({"polnum":1, "poledi":1}, { "unique": True } )


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
                    # if self.db["contrats"].find_one({"poledi": c.poledi, "polnum": c.polnum}) is not None: 
                    self.db["contrats"].insert_one(c.__dict__)
                else:
                    try:
                        row["POLNUM"] = row["POLNUM"].strip()
                        c = Contrat(row)
                        # if self.db["contrats"].find_one({"poledi": c.poledi, "polnum": c.polnum}) is not None: 
                        self.db["contrats"].insert_one(c.__dict__)
                        # self.db["contrats"].insert_one(c.__dict__)
                    except Exception as e:
                        print(i+1, e)
                        continue
    
    def get_distinct_police_editions(self):
        return self.db.contrats.distinct("poledi")      
        
    def get_distinct_police_numbers(self):
        return self.db.contrats.distinct("polnum")
    
    def group_police_number_by_cie(self):
        self.db['cie_police_number'].drop()
        self.db.contrats.aggregate([
            { "$group" : { "_id" : "$cienom", "police_numbers": { "$push": "$polnum" } } },
            { "$out" : "cie_police_number" }
        ])
        return self.db['cie_police_number'].find()
    
    def group_police_edition_by_cie(self):
        self.db['cie_police_edition'].drop()
        self.db.contrats.aggregate([
            { "$group" : { "_id" : "$cie.name", "police_editions": { "$push": "$poledi" } } },
            { "$out" : "cie_police_edition" }
        ])
        return self.db.cie_police_edition.find({})
    
    def group_contrat_by_cie(self):
        self.db['cie_contrats'].drop()
        self.db.contrats.aggregate([
            { "$group" : { "_id" : "$cie.name", "total":{"$sum":1}, "contrats": { "$push": "$$ROOT" } } },
            { "$out" : "cie_contrats" }
        ])
        return self.db.cie_contrats.find({})
    
    def group_document_by_cie(self):
        self.db['cie_documents'].drop()
        self.db.documents.aggregate([
            { "$group" : { "_id" : "$cie.name", "total":{"$sum":1}, "documents": { "$push": "$filename" } } },
            { "$out" : "cie_documents" }
        ])
        return self.db.cie_documents.find({})
    
    # def get_ref_not_found_in_docs_by_cie(self, cie_name="HUMANIS"):
        
    #     for doc in self.db.documents.find({'cie.name':cie_name, 'ref': None}):
    #         print(doc["filename"], doc["ref"])
    #         n = re.search(r"\s(n|N)°(?P<ref>.?\d*)\s", doc["text"])
    #         print(n)
            # d = Document(doc["input_filepath"])
            # d.get_match()
            # if d.ref is not None:
            #     self.search_doc_in_contracts(d)
            # self.db.documents.update_one({"_id": doc["_id"]}, {"$set":d.__document__()})
            

if __name__ == "__main__":
    db = DB("AVENANTS_2")
    # db.get_ref_not_found_in_docs_by_cie()
    # db.reset()
    db.init()
    # db.preprocess()
    # db.populate_documents_from_fs("./avenants_input/")
    # db.group_document_by_cie()
    # db.group_contrat_by_cie()
    # print(db.db['documents'].find_one())
    # print(db.db['cie_contrats'].distinct("_id"))
    # print(db.db['cie_contrats'].find_one({"_id": "HUMANIS"}))
    # print(db.db['cie_documents'].find_one({"_id": "MUTUELLE GENERALE"}))
    # db.matching_cies()
    # db.get_contrats_by_cie("UNIPREVOYANCE")
    #db.get_document_by_cie("UNIPREVOYANCE")
    # for cie in db.db.cie_documents.dictinct("_id"):
    #     print(cie)
    