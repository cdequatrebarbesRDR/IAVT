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
from collections import defaultdict

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

    def process_from_fs(self,docs_dir="./avenants_input/", reset=True, export=True):
        '''From folder'''
        self.populate_documents_from_folder(docs_dir, reset)
        return self
    def stats(self):
        print("******************************************")
        self.nb_contrat = self.db.contrats.count_documents({})
        self.nb_doc = self.db.documents.count_documents({}) 
        print(f"\t-Nb documents: {self.nb_doc}")
        self.nb_ref = self.db.documents.count_documents({"ref":{"$ne":None}}) 
        self.nb_poledi = self.db.documents.count_documents({"poledi":{"$ne":None}})
        self.nb_numper = self.db.documents.count_documents({"numper":{"$ne":None}})
        print(f"\t- Documents avec une référence de contrat dans le texte:{self.nb_ref}")
        print(f"\t- Documents avec un numéro de contrat dans la base:{self.nb_poledi}")
        print(f"\t- Documents avec un numéro de periode:{self.nb_numper}")
        self.ratio_ref = float(self.nb_ref/self.nb_doc)
        self.ratio_poledi = float(self.nb_poledi/self.nb_ref)
        self.ratio_numper = float(self.nb_numper / self.nb_poledi)
        self.global_score = float(self.nb_numper / self.nb_doc)
        print(f"% de detection du n° de contrat dans le texte : {self.ratio_ref * 100} %")
        print(f"% de detection du n° de contrat en base : {self.ratio_poledi * 100} %")
        print(f"% de reconciliation N° de contrat - N° de police   {self.ratio_numper * 100} %")
        print(f"Soit un % de réconciliation  {self.global_score * 100} %")
        print("******************************************")
        return self
    
    def process_from_db(self,reset=True, stats=True):
        for doc in self.db.documents.find():
            d = Document(doc)
            #self.search_doc_in_contracts(d)
            self.match_doc(d)    
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
    def get_police_edition(self, ref, cie_name):
        contrat_items = self.db["contrats"].find({
            "$or":[{"poledi": {"$regex":ref}}, {"polnum":{"$regex": ref}}]
        })
        poledis = list(set([c["poledi"] for c in contrat_items]))
        if len(poledis) == 0:
            #pas trouvé dans la base à n'executer qu'une seule fois
            try:
                if refined is not True and cie_name == "GROUPAMA":
                    refined = True
                    new_ref = ref.split("/")
                    if len(new_ref) == 2:    
                        return self.get_police_edition(new_ref[1], cie_name)
                return False, 0, None
            except NameError:
                return False, 0, None
        if len(poledis) == 1:
            return True, 1, poledis[0]
        else:
            # import jellyfish
            import editdistance
            poledis = contrat_items.distinct("poledi")
            for poledi in poledis:
                #tdfidf_ratio = jellyfish.levenshtein_distance(ref, poledi)
                lev = editdistance.eval(ref, poledi)
                print(ref, poledi, "distance", lev)
                # if ref in poledi:
                #     print(ref, poledi, "INSIDE")
                #     return True, 1, poledi 

        return False, len(poledis), poledis
      
    def get_police_edition_number(self, d):
        
        contrat_items = self.db["contrats"].find({
            "$or":[{"poledi": {"$regex":d.ref}}, {"polnum":{"$regex": d.ref}}]
        })
        poledi_nb_count = len(contrat_items.distinct("poledi"))
        if poledi_nb_count == 0:
            #pas trouvé dans la base
            if not hasattr(d, "original_ref"): 
                if d.cie.name == "GROUPAMA":
                    d.original_ref = d.ref
                    d.ref = d.ref.split("/")[1]
                    d.matches["original_ref"] = d.original_ref
                    return self.get_police_edition_nb(self, d)
            return False, 0, None
        if poledi_nb_count == 1:
            poledi = contrat_items[0]["poledi"]
            return True, 1, poledi
        else:
            # import jellyfish
            import editdistance
            poledis = contrat_items.distinct("poledi")
            for poledi in poledis:
                #tdfidf_ratio = jellyfish.levenshtein_distance(ref, poledi)
                lev = editdistance.eval(d.ref, poledi)
                print(d.ref, poledi, lev)
                if d.ref in poledi:
                    print(d.ref, poledi, "INSIDE")
                    return True, poledi_nb_count, poledi 

        return False, poledi_nb_count, poledis
        #yield from [(True, poledi, "X numéro de Contrat") from poledi in police_nbs]
                
    def get_numpers_by_police_edition_number(self, poledi):
        if self.db.poledis.count_documents({"_id": poledi}) > 1:
            raise Exception("Poledi is supposed to be unique")
        numper_cat_codes = self.db.poledis.find_one({"_id": poledi})
        return numper_cat_codes["periodes"]
    
    def filter_numpers_by_catcode_blacklist(self, numpers, blacklist=["Z", "X", "K"]):
        '''blacklist: X, K, Z'''
        return [n for n in numpers if n["catcod"][0] not in blacklist]
    
    def filter_numpers_by_catcode_whitelist(self, numpers, prioritylist=["ASS", "ENS", "CAD", "NC", "ETM", "TNS"]):
        '''whitelist: priority code from generic to specific'''
        return [n for n in numpers if n["catcod"] in prioritylist]
    
    def get_unique_catcodes(self, numpers):
        catcods = defaultdict.fromkeys([(n["fam"], n["catcod"]) for n in numpers], [])
        for key in catcods:
            catcods[key] = [n for n in numpers if key == (n["fam"], n["catcod"])]
        return catcods
    
    def find_numper_by_police_edition_number(self, poledi):
        '''
        Input: poledi
        output: find_numper, numper, comment
        '''
        numpers  = self.get_numpers_by_police_edition_number(poledi)
        numpers_nb = len(numpers)
        if numpers_nb == 0:
            return False, numpers_nb, None, numpers, "Pas de numéro de période"
        if numpers_nb == 1:
            return True, numpers_nb, numpers[0], numpers, "Un seul numéro de période"
        filtered_numpers = self.filter_numpers_by_catcode_blacklist(numpers)
        if len(filtered_numpers) == 0:
            by_cat_and_fam = list(set([(n["fam"], n["catcod"]) for n in numpers]))
            unique_catcode_fam_numpers = [n for n in numpers if (n["fam"], n["catcod"]) in by_cat_and_fam]
            
            if len(unique_catcode_fam_numpers) != len(numpers):
                return False, len(unique_catcode_fam_numpers), unique_catcode_fam_numpers, numpers, "Numéro de période invalide en doublon"
            return False, len(filtered_numpers), filtered_numpers, numpers, "Aucun de numéro de période valide"
        
        if len(filtered_numpers) == 1:
            return True, len(filtered_numpers), filtered_numpers[0], numpers, "Un numéro de période valide"
        else:
            
            by_cat_and_fam = list(set([(n["fam"], n["catcod"]) for n in filtered_numpers]))
            if len(by_cat_and_fam) < len(filtered_numpers):
                return False, len(filtered_numpers), filtered_numpers, numpers, "Numéro de période en doublon"    
            re_filtered_numpers = self.filter_numpers_by_catcode_whitelist(numpers)
            if len(re_filtered_numpers) == 1:
                return True, len(re_filtered_numpers), re_filtered_numpers[0], numpers, "Un numéro de période valide"
            return False, len(filtered_numpers), filtered_numpers, numpers, "Plusieurs numéro de période valides"
    
    def get_contrat_by_period_number(self, numper):
        return self.db["contrats"].find_one({"numper": numper})
    
    def search_contrat(self, ref, cie_name):
        found, nb_match, numero_police = self.get_police_edition(ref, cie_name)
        if found: 
            if nb_match == 1:
                status, nb_matches, result, records, comment =  self.find_numper_by_police_edition_number(numero_police)
                if status:
                    return (status, self.get_contrat_by_period_number(result["numper"]))
                else:
                    if result is not None:
                        print("MULTIPLE NUMPERS", nb_matches, result, comment)        
                return (False, None)
            else:
                print("MULTIPLE POLICE", nb_match, numero_police)
                return (False, None)
        return (found, None)

    # def match_doc(self, d):
    #     d.status = False
    #     d.found_ref = False
    #     d.new_filename = None
    #     d.comment = "Pas de référence à un n° de contrat trouvé dans le texte"
    #     if d.ref is not None:
    #         d.found_ref = True
    #         d.found_poledi, d.poledi, d.comment = self.get_police_edition_number(d)
    #         if d.found_poledi:
    #             d.found_numper, d.numper, d.comment= self.get_numpers_by_police_edition_number(d.poledi)
    #             if d.found_numper:
    #                 d.status = True
    #                 d.new_filename = f"{d.poledi}-{d.numper["numper"]}.pdf"
                     
    #     print("\t".join([str(d.filename),str(d.new_filename), str(d.status), str(d.numper), d.comment]))
    #     self.store_doc(d)
    #     return d.status
            


    
    def store_doc(self, d, reset=False):
        if reset:
            self.db["documents"].insert_one(d.__export__())
            return d
        record = self.db.documents.find_one({"filename": d.filename}, {"_id":1})
        if record is not None:
            self.db["documents"].update_one({"_id": record["_id"]}, {"$set":d.__export__()}, True)
        else:
            self.db["documents"].insert_one(d.__export__())
        return d
    
    def populate_documents_from_folder(self, input_dir="./avenants_input/", reset = False):
        print(f"INSERTING DOCS from {input_dir} INTO DB and FOLDERS")
        # if reset:
        self.db.documents.drop()
        
        for filepath in glob(os.path.join(input_dir, '**', '*.pdf'), recursive=True):
            #if still ALLIANZ in documents
            print(filepath)
            if not "AZ" in filepath:
                d = Document(filepath)
                #self.match_doc(d)
                # d.match_contrat()
                if d.ref is not None:
                    self.search_contrat(d.ref, d.cie.name)
                d.store(True)
        #self.stats()

                
                
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
        print(f"\t-Nb de contrats: {self.nb_police}")
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
    db = DB("CONTRATS", False)
    #db.process_from_fs()
    db.populate_documents_from_folder("./avenants_input/", True)
    db.stats()