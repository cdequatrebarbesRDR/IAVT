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
    def __init__(self, DB_NAME="AVENANTS_3", reset=True):
        #self.client = AsyncMongoClient("mongodb://localhost:27017/")
        self.name = DB_NAME
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[DB_NAME]
        if reset:
            self.populate_contrats_from_csv("TK2501333.csv", reset=reset)
        self.contrats = self.db.contrats
        self.documents = self.db.documents

    def process_from_fs(self,docs_dir="./avenants_input/", results_dir="./results/", reset=True, export=True):
        '''From folder'''
        self.populate_documents_from_folder(docs_dir, True)
        
        if export:
            self.export_documents(docs_dir, results_dir)
        return self
    def stats(self):
        self.nb_contrat = self.db.contrats.count_documents({})
        self.nb_doc = self.db.documents.count_documents({}) 
        print(f"\t-Nb documents: {self.nb_doc}")
        self.nb_ref = self.db.documents.count_documents({"ref":{"$ne":None}}) 
        self.nb_poledi = self.db.documents.count_documents({"poledi":{"$ne":None}})
        self.nb_numper = self.db.documents.count_documents({"numper":{"$ne":None}})
        print(f"\t- Documents avec une référence de contrat dans le texte:{self.nb_ref}")
        print(f"\t- Documents avec un numéro de contrat dans la base:{self.nb_found}")
        print(f"\t- Documents avec un numéro de periode:{self.nb_match}")
        self.ratio_ref = float(self.nb_ref/self.nb_doc)
        self.ratio_poledi = float(self.nb_poledi/self.nb_ref)
        self.ratio_numper = float(self.nb_numper / self.nb_poledi)
        self.global_score = float(self.nb_numper / self.nb_doc)
        print(f"% de detection du n° de contrat dans le texte : {self.ratio_ref * 100} %")
        print(f"% de detection du n° de contrat en base : {self.ratio_poledi * 100} %")
        print(f"% de reconciliation N° de contraty   {self.ratio_numper * 100} %")
        print(f"Soit un % de réconciliation  {self.global_score * 100} %")
        return self
    
    def process_from_db(self,reset=True, stats=True):
        for doc in self.db.documents.find():
            d = Document(doc)
            #self.search_doc_in_contracts(d)
            self.match_doc(d)    
        print(f"\t-Nb documents: {self.nb_doc}")
        self.nb_ref = self.db.documents.count_documents({"ref":{"$ne":None}}) 
        self.nb_found = self.db.documents.count_documents({"poledi": True})
        self.nb_perfect_match = self.db.documents.count_documents({"numper":{"$ne":None}})
        print(f"\t- Documents avec une référence de contrat:{self.nb_ref}")
        print(f"\t- Documents appairés:{self.nb_found}")
        self.ratio_found = float(self.nb_found/self.nb_ref)
        self.ratio_ref =float(self.nb_ref / self.nb_doc)
        self.global_score = float(self.nb_found / self.nb_doc)
        print(f"Soit un % de correspondance global de {self.global_score * 100} %")
        # if export:
        #     self.export_documents(docs_dir, results_dir)
        return self

    def reset(self):
        self.db["documents"].drop()
        self.db["contrats"].drop()
        if os.path.exists("./results"):
            os.removedirs("./results")
   
    
    def get_police_nb(self, query, d):
        '''Un numero de police dans la base de contrats?'''
        nb_match = self.db["contrats"].count_documents(query)
        poledis = self.db["contrats"].find(query).distinct("poledis")
        if nb_match == 0:
            d.found = False
            if not hasattr(d, "original_ref"): 
                if d.cie.name == "GROUPAMA":
                    d.original_ref = d.ref
                    d.ref = d.ref.split("/")[1]
                    d.matches["original_ref"] = d.original_ref
                    return self.get_police_nb(self, query, d)
                else:
                    d.original_ref = d.ref
            d.matches = {"ref": d.ref, "original_ref": d.original_match, "poledi": {"count": nb_match,"poledis": poledis}}
            d.matches["original_ref"] = d.original_ref
            return (d.found, d)    
        else:
            if not hasattr(d, "original_ref"):
                d.original_ref = d.ref
            d.found  = True
            d.matches = {"ref": d.ref, "original_ref": d.original_ref, "poledi": {"count": nb_match,"poledis": poledis}}
            if nb_match == 1:
                d.poledi = poledis[0]        
            else:
                # Choose num police?
                d.poledi = None
            return (d.found, d)
        
    def get_poledi_by_ref(self, ref):
        
        contrat_items = self.db["contrats"].find({
            "$or":[{"poledi": {"$regex":ref}}, {"$regex":{"polnum": ref}}]
        })
        police_nbs = contrat_items.distinct("poledi")
        police_nb_count = len(police_nbs)
        if police_nb_count == 0:
            #pas trouvé dans la base
            return False, None, "Pas de numéro de Contrat trouvé dans la base"
        if police_nb_count == 1:
            poledi = police_nbs[0]
            return True, poledi, "Un numéro de Contrat trouvé dans la base"      
        return False, None, f"{police_nb_count} numéro de Conntrat trouvés dans la base: lequel choisir? {police_nbs}"
        #yield from [(True, poledi, "X numéro de Contrat") from poledi in police_nbs]
                
    def get_numper_catcod_fam_by_poledi(self, poledi):
        if self.db.poledis.count_documents({"_id": poledi}) > 1:
            raise Exception("Poledi is supposed to be unique")
        numper_cat_codes = self.db.poledis.find_one({"_id": poledi})
        return numper_cat_codes["periodes"]
    
    def filter_numpers_by_catcode(self, records):
        '''blacklist: X, K, Z'''
        return [n for n in records if n["catcode"][0] not in ["Z", "X", "K"]]
        
    def get_numper_by_poledi(self, poledi):
        numpers  = self.get_numper_catcod_fam_by_poledi(poledi)
        if len(numpers) == 0:
            return False, None, "No numper found for this poledi"
        if len(numpers) == 1:
            return True, numpers[0], "One single numper for this poledi"
        filtered_numpers = self.filter_numpers_by_catcode(numpers)
        if len(filtered_numpers) == 0:
            return False, None, "No valid numper found for this poledi: {numpers}"
        
        if len(filtered_numpers) == 1:
            return True, numpers[0], "One single valid numper for this poledi"
        else:
            
            return False, None, "Please choose the valid numper given catcod: {filtered_numpers}"
    
    def match_doc(self, d):
        d.status = False
        d.found_ref = False
        d.comment = "Pas de référence à un n° de contrat trouvé dans le texte"
        if d.ref is not None:
            d.found_ref = True
            d.found_poledi, d.poledi, d.comment = self.get_poledi_by_ref(d.ref)
            if d.found_poledi:
                d.found_numper, d.numper, d.commentget_numper_by_poledi(self, d.poledi)
                if d.found_numper:
                    d.status = True
                    d.new_filename = f"{d.poledi}_{d.numper}.pdf"
                     
        print(d.filename, d.status, d.numper, d.comment)
        self.store_doc(d)
        return d.status
            


    # def qualify_doc(self, query, d):
    #     '''Method to match'''
    #     nb_match = self.db["contrats"].count_documents(query)
    #     if nb_match == 0:
    #         d.found = False
    #         d.numper = None
    #         #retry for Groupama with chunk
    #         if not hasattr(d, "original_ref") and d.cie.name == "GROUPAMA":
    #             d.original_ref = d.ref
    #             d.ref = d.ref.split("/")[1]
    #             return self.qualify_doc(self, query, d)
    #         #else simply not found
    #         print(d.filename, ": contrat not found", d.found, "with ref", d.ref, "for CIE", d.cie.name)
    #         return (d.found, d)
    #     poledis = self.db["contrats"].find(query).distinct('poledi')
    #     if len(poledis) > 1:
    #         print("Police edition is not unique, stretching query with exact match")
    #         stretch_query = {
    #             "$or":[
    #                 {"poledi":d.ref}, 
    #                 {"polnum":d.ref}
    #             ]
    #         }
    #         return self.qualify_doc(stretch_query, d)
    #     if len(poledis) == 1:
    #         d.found = True
    #         catcodes = self.db["contrats"].find(query).distinct('catcod')
    #         fams = self.db["contrats"].find(query).distinct('fam')
    #         d.matches = {"count": nb_match, "poledis": poledis, "numpers": numperiodes, "catcods": catcodes, "fams": fams}
    #         numperiodes = self.db["contrats"].find(query).distinct('numper')
    #         if len(numperiodes) == 1:
    #             first_match = self.db["contrats"].find_one(query)
    #             d.catcod = first_match['catcod']
    #             d.prd = first_match['prd']
    #             d.opt = first_match['opt']
    #             d.fam = first_match['fam']
    #             d.poledi = poledis[0]
    #             d.polnum = first_match["polnum"]
    #             d.entrai = first_match["entrai"]
    #             d.numper = numperiodes[0]
    #             print(d.filename, ": contrat found", d.found, "with ref", d.ref, "for CIE", d.cie.name, "with numper", d.numper, d.catcod, d.fam)
    #             return (self.found, d)
    #         else:


    #         d.poledi = poledis[0]
    #         d.polnum = first_match["polnum"]
    #         d.entrai = first_match["entrai"]
    #         d.numper = None
    #         print(d.filename, ": contrat found", d.found, "with ref", d.ref, "for CIE", d.cie.name, "with numper to choose with catcod and fam", catcodes, fams)
    #         return (self.found, d)
            

    # def search_doc_in_contracts(self, d):
    #     d.found = False
    #     if d.ref is None:
    #         return (d.found, d)
    #     query =  {
    #             "$or":[
    #                 {"poledi":{"$regex":d.ref}}, 
    #                 {"polnum":{"$regex":d.ref}}
    #             ]
    #         }
    #     return self.qualify_doc(query, d)
        
    def store_doc(self, d, reset=False):
        if reset:
            self.db["documents"].insert_one(d.__document__())
            return d
        record = self.db.documents.find_one({"filename": d.filename}, {"_id":1})
        if record is not None:
            self.db["documents"].update_one({"_id": record["_id"]}, {"$set":d.__document__()}, True)
        else:
            self.db["documents"].insert_one(d.__document__())
        return d
    
    def populate_documents_from_folder(self, input_dir="./avenants_input/", reset = False):
        print(f"INSERTING DOCS from {input_dir} INTO DB and FOLDERS")
        if reset:
            self.db.documents.drop()
        for filepath in glob(os.path.join(input_dir, '**', '*.pdf'), recursive=True):
            #if still ALLIANZ in documents
            if not "AZ" in filepath:
                d = Document(filepath)
                self.match_doc(d)
                # self.search_doc_in_contracts(d)
                # self.store_doc(d)

                
                
    def export_documents_from_fs(self, input_dir="./avenants_input/", output_dir= "./results/"):
        for filepath in glob(os.path.join(input_dir, '**', '*.pdf'), recursive=True):
            #if still ALLIANZ in documents
            if not "AZ" in filepath:
                d = Document(filepath)
                ko_dir = os.makedirs(os.Path.join(os.getcwd(), "KO", d.cie.name))
                self.search_doc_in_contracts(d)
                # self.store_doc(d)
                if d.found:
                    d.output_filename = f"{d.numper}.pdf"
                    d.output_filepath = os.path.join(output_dir, d.ouput_filepath)
                    shutil.copy(d.input_filepath, d.output_filepath)
                    DB.stats.insert_one({
                        "status": "OK", 
                        "cie.name": d.cie.name, 
                        "input_filepath": d.input_filepath, 
                        "output_filepath":d.output_filepath,
                        "reference": d.ref,
                        "poledi": d.poledi,
                        "entrai": d.entrai,
                        "polnum": d.polnum,
                        "numper": d.numper
                        })
                else:
                    d.output_filename = f"{d.filename}.pdf"
                    d.output_filepath = os.path.join(ko_dir, d.ouput_filepath)
                    shutil.copy(d.input_filepath, d.output_filepath)
                    if d.ref is not None:
                        DB.stats.insert_one({
                            "status": "KO", 
                            "cie.name": d.cie.name, 
                            "input_filepath": d.input_filepath, 
                            "reference": d.ref,
                            "commentaire": "Référence non trouvée dans la base de contrats."
                            })
                    else:
                        DB.stats.insert_one({
                            "status": "KO", 
                            "cie.name": d.cie.name, 
                            "input_filepath": d.input_filepath, 
                            "reference": d.ref,
                            "commentaire": "Référence non détectée dans le texte."
                            })
                

    def populate_contrats_from_csv(self, csv_filename="TK2501333.csv", reset=False, delimiter=";"):
        if reset:
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
                    # if self.db["contrats"].find_one({"poledi": c.poledi, "polnum": c.polnum}) is not None: 
                    self.db["contrats"].insert_one(c.__document__())
                else:
                    try:
                        row["POLNUM"] = row["POLNUM"].strip()
                        c = Contrat(row)
                        # if self.db["contrats"].find_one({"poledi": c.poledi, "polnum": c.polnum}) is not None: 
                        self.db["contrats"].insert_one(c.__document__())
                        # self.db["contrats"].insert_one(c.__dict__)
                    except Exception as e:
                        print(i+1, e)
                        continue
        self.nb_contrat = self.db.contrats.count_documents({})
        print(f"\t-Nb periodes de contrats: {self.nb_contrat}")
        self.db.contrats.aggregate([
            { "$group" : 
                { 
                    "_id" : "$poledi", 
                    "periodes": {"$push": {"numper": "$numper", "fam": "$fam", "catcod": "$catcod"}} 
                }
            },
            { "$out" : "poledis" }
        ])
        self.db.poledis = self.db["poledis"]
        self.nb_police = self.db.poledis.count_documents({})
        print(f"\t-Nb de contrats: {self.nb_contrat}")
        return self
    
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
    
    def search_numper_by_poledi(self, poledi: str):
        return self.db.poledis.find({"_id": poledi})
    
    def get_catcodes_by_poledi(self, poledi:str):
        numpers = self.search_numper_by_poledi(poledi)
        if len(numpers) is not None:
            return {
                poledi:[
                    {
                    "numper":numper, 
                    "catcodes":self.db.contrats.find({"numper": numper}).distinct('catcod')
                    }
                    for numper in numpers
                ]
                }

    def get_numper_by_catcode_and_poledi(self, poledi:str, catcod:str ):
        return self.db.contrats.find({"poledi": poledi, "catcod":catcod}) 

if __name__ == "__main__":
    db = DB("AVENANTS_CONTRATS", True)
    db.process_from_fs()
    