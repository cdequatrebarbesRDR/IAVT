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
from models import Compagnie
from collections import defaultdict
import pandas
import pymongo
from typing import List, Union

class DB:
    def __init__(self, DB_NAME="contrat_docs", init=False):
        #self.client = AsyncMongoClient("mongodb://localhost:27017/")
        self.name = DB_NAME
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[DB_NAME]
        self.documents = self.db["documents"]
        if init:
            self.create_table_contrats("TK2501333.csv")
            self.create_table_poledis()
            self.create_table_polnums()
            # self.create_table_compagnies()
            # self.create_table_candidates()
        
    
    def stats(self):
        print("******************************************")
        self.nb_contrat = self.db.contrats.count_documents({})
        self.nb_doc = self.db.documents.count_documents({}) 
        print(f"\t-Nb documents: {self.nb_doc}")
        self.nb_text = self.db.documents.count_documents({"text":{"$ne":None}})
        self.nb_compagnies = self.db.documents.count_documents({"cie":{"$ne":None}}) 
        self.nb_ref = self.db.documents.count_documents({"ref":{"$ne":None}}) 
        self.nb_poledi = self.db.documents.count_documents({"police":{"$ne":None}})
        # self.nb_polnum = self.db.documents.count_documents({"polnum":{"$ne":None}})
        self.nb_numper = self.db.documents.count_documents({"numper":{"$ne":None}})
        print(f"\t- Documents avec du texte:{self.nb_text}")
        print(f"\t- Documents avec une compagnie:{self.nb_compagnies}")
        print(f"\t- Documents avec une référence de contrat dans le texte:{self.nb_ref}")
        print(f"\t- Documents avec un numéro de contrat dans la base:{self.nb_poledi}")
        print(f"\t- Documents avec un numéro de periode:{self.nb_numper}")
        if self.nb_doc != 0:
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
    #INIT
    def create_table_contrats(self, init=False, csv_filename="TK2501333.csv", delimiter=";"):
        '''Create Table CONTRATS'''
        if init:
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
                    self.db["contrats"].insert_one(c.__export__())
                else:
                    try:
                        row["POLNUM"] = row["POLNUM"].strip()
                        c = Contrat(row)
                        self.db["contrats"].insert_one(c.__export__())
                    except Exception as e:
                        print("Exception at row", i+1, e, row)
                        continue
        self.nb_contrat = self.db.contrats.count_documents({})
        print(f"\t-Nb de contrats: {self.nb_contrat}")
        return self
    
    def create_table_poledis(self):
        '''
        create a Table for poledis from Contrats unique police editions with multiple periodes
        '''
        if self.nb_contrat == 0:
            self.create_table_contrats()
        self.db.contrats.aggregate([
            { "$group" : 
                { 
                    "_id" : "$poledi",
                    "periodes": {"$push": {
                            "numper": "$numper", 
                            "fam": "$fam", 
                            "catcod": "$catcod", 
                            "poledi": "$poledi", 
                            "polnum": "$polnum", 
                            "entrai": "$entrai",
                            "entnum": "$entnum"
                        }
                    }
                
                }
            },
            { "$out" : "poledis" }
        ])
        self.db.poledis = self.db["poledis"]
        self.nb_police = self.db.poledis.count_documents({})
        print(f"\t-Nb de numéro de police d'édition: {self.nb_police}")
        return self

    def create_table_polnums(self):
        '''
        create a Table for polnum from Contrats unique police number with multiple periodes
        '''
        self.db.contrats.aggregate([
            { "$group" : 
                { 
                    "_id" : "$polnum",
                    "periodes": {"$push": {
                            "numper": "$numper", "fam": "$fam", "catcod": "$catcod", 
                            "poledi": "$poledi", 
                            "polnum": "$polnum", 
                            "entrai": "$entrai",
                            "entnum": "$entnum"
                        }
                    }
                
                }
            },
            { "$out" : "polnums" }
        ])
        self.db.polnums = self.db["polnums"]
        self.nb_polnums = self.db.polnums.count_documents({})
        print(f"\t-Nb de numéro de police: {self.nb_polnums}")
        return self
    
    def create_table_compagnies(self):
        '''Create TABLE COMPAGNIE from Contrats and index rules from Compagnie'''
        if self.nb_contrat == 0:
            self.create_table_contrats()
        self.db.contrats.aggregate([
            { "$group" : 
                { 
                    "_id" : "$cienom",
                    
                }
            },
            { "$out" : "compagnies" }
        ])
        self.db.compagnies = self.db["compagnies"]
        self.nb_compagnies = self.db.compagnies.count_documents({})
        print(f"\t-Nb de Compagnies: {self.nb_compagnies}")
        for c in self.db.compagnies.find({}):
            c1 = Compagnie(c["_id"])
            self.db.compagnies.insert_one(c1.__export__(), True)
        self.nb_compagnies_implemented = self.db.compagnies.count_documents({"name": {"$ne": None}})
        print(f"\t-Nb de Compagnies avec des règles: {self.nb_compagnies}")
        return self

    def create_table_candidates(self, input_dir:str, reset=True):
        '''Create TABLE CANDIDATES from Folder'''
        if reset:
            self.db.candidats.drop()
        self.db.candidats = self.db["candidats"]
        self.db.candidats.create_index([('filepath', pymongo.TEXT)])
        for filepath in glob(os.path.join(input_dir, '**', '*.pdf'), recursive=True):
            #Regles de filtrages: pas ALLIANZ pas KO
            if not "az" in filepath or not "AZ" in filepath or not "KO" in filepath or not "ALLIANZ" in filepath:
                try:
                    self.db.candidats.insert_one({"filepath": filepath}, True)
                except pymongo.errors.DuplicateKeyError:
                    pass
        self.nb_candidats = self.db.candidats.count_documents({})
        print(f"\t-Nb de candidats: {self.nb_candidats}")
        return self

    #OLD              
    def search_poledi(self, ref:str)->list:
        poledis_nb = self.db.poledis.count_documents({"_id": {"$regex": ref}})
        if poledis_nb == 0:
            return (poledis_nb, None)
        elif poledis_nb == 1:
            poledi_item = self.db.poledis.find_one({"_id": {"$regex": ref}})
            return (poledis_nb,[poledi_item])
        else:
            poledi_items = self.db.poledis.find({"_id": {"$regex": ref}})
            return (poledis_nb, list(poledi_items))
    
    def search_polnum(self, ref:str)->list:
        polenum_nb = self.db.polnums.count_documents({"_id": {"$regex": ref}})
        if polenum_nb == 0:
            return (polenum_nb, None)
        elif polenum_nb  == 1:
            polenum_item = self.db.polnums.find_one({"_id": {"$regex": ref}})
            return (polenum_nb, [polenum_item])
        else:
            polenum_items = self.db.polnums.find({"_id": {"$regex": ref}})
            return (polenum_nb, list(polenum_items))
    
    def get_contrats_by_police_nb(self, police_nb:str)-> List[dict]:
        contrats = []
        for c in self.db.contrats.find({"$or":[{"polnum": {"$regex": police_nb}}, {"poledi": {"$regex": police_nb}}]}):
            contrats.append(c)
        return contrats
    
    def get_contrat_by_period_nb(self, police_nb:str)-> dict:
        return self.db.contrats.find_one({"numper": police_nb})
        
    def get_contrats_by_period_nb(self, period_nb:str)-> List[dict]:
        contrats = []
        for c in self.db.contrats.find({"numper": period_nb}):
            contrats.append(c)
        return contrats
    
    def order_numper_by_fam_and_catcod(self, contrats: List[dict])-> dict:
        '''Ordonner les numéro de periodes par famille et catcod'''
        catcods = defaultdict.fromkeys([(n.fam, n.catcod) for n in contrats], [])
        for key in catcods:
            catcods[key] = [c for c in contrats if key == (c["fam"], c["catcod"])]
        return catcods
    
    def blacklist_catcods(self, catcods: dict, blacklist:list=["Z", "X", "K"])-> dict:
        '''Supprimer les contrats qui concernent certains colleges'''
        f_catcods = {}
        for key, values in catcods.items():
            #fam,catcod = key
            if key[1][0] not in blacklist:
                f_catcods[key] = values
        if len(f_catcods) == 0:
            return catcods
        return f_catcods

    def whitelist_catcods(self, catcods: dict, whitelist:list=["ASS", "ENS", "CAD", "NC", "ETM", "TNS"])-> dict:
        '''Selectionner les contrats qui concernent certains colleges'''
        f_catcods = {}
        for key, values in catcods.items():
            #fam,catcod = key
            if key[1] not in whitelist:
                f_catcods[key] = values
        if len(f_catcods) == 0:
            return catcods
        return f_catcods
    

    def filter_contrats_by_catcod(self, contrats:List[dict])-> List[dict]:
        '''
        Filtrer les contrats par périodes:
        - fam,catcod uniques
        - valides: pas de catcod commençant K Z X
        - generiques: les catcods les plus generiques ['ENS', 'ASS', 'CAD','NC', 'CA*' ]
        '''
        catcodes = self.order_numper_by_fam_and_catcod(contrats)
        if len(catcodes) ==1:
            return catcodes.values()
        
        b_catcodes = self.blacklist_catcods(catcodes)
        if len(b_catcodes) == 1:
            return b_catcodes.values()
        if b_catcodes is not None:
            catcodes = b_catcodes        
        w_catcodes = self.whitelist_catcods(catcodes)
        if len(w_catcodes) ==1:
            return w_catcodes.values()
        if w_catcodes is not None:
            catcodes = w_catcodes
        return catcodes.values()
            
    def get_contrat_by_police_nb(self, police_nb:str)-> list:
        '''A partir d'un numéro de contrat renvoyé Trouvé et le Contrat'''
        polices = self.get_contrats_by_police_nb(police_nb)
        contrats_nb = len(polices)
        if contrats_nb == 0:
            print(f"Aucun contrat avec le numéro de police {police_nb} trouvé")
            return False, None
        if 6< contrats_nb > 1:
            contrats = []
            for record in polices:
                numpers.append(self.get_contrats_by_period_nb(record["numper"]))
            print(f"Plusieurs contrats avec le numéro de police {police_nb} trouvés: {contrats_nb}")
            return False, polices
        if contrats_nb == 1:
            contrats = self.get_contrats_by_period_nb(polices[0]["numper"])
            contrats_nb = len(contrats)
            if contrats_nb == 0:
                return False, polices
            if contrats_nb == 1:
                return True, contrats
            if contrats_nb > 1:
                periodes = [c["numper"] for c in contrats]            
                print(f"Plusieurs contrats avec numéros de périodes {periodes} trouvés: {contrats_nb}")
                filter_contrats = self.filter_contrats_by_catcod(contrats)
                if len(filter_contrats) == 0:
                    print(f"Aucun contrat trouvé avec un numéro de période {periodes} valide trouvés parmi les {contrats_nb}")
                    return False, contrats
                if len(filter_contrats) == 1:
                    return True, filter_contrats
                periodes = [c["numper"] for c in filter_contrats]
                print(f"Plusieurs contrats filtrés avec numéros de périodes valide trouvés parmi les {contrats_nb}")
                return False, filter_contrats

        return False, contrats

    def index_document(self, filepath)-> Document:
        '''Indexer un document en le reliant à un contrat'''
        d = Document(filepath)
        d.online_filepath = d.filepath.replace("./", "S://Contrat/1 - INDEXATIONS/")
        d.status = False
        d.comment = "Pas de numéro de contrat trouvé avec cette référence"
        d.contrats_nb = 0
        d.contrat = None          
        if d.ref is not None:
            d.found, contrats = self.get_contrat_by_police_nb(d.ref)
            d.status = d.found
            if d.found:
                print(contrats)
                d.contrat = contrats[0]
                d.contrats_nb = 1
                d.comment ="OK"
                d.new_filename =  f"{contrats[0]['numper']}.pdf"
                
            else:
                if contrats is not None:
                    d.comment = "Impossible de choisir parmi les contrats"
                    d.contrats = contrats
                    d.contrats_nb = len(contrats)
                
        self.db.documents.insert_one(d.__export__())
        return d 
        
    def index_documents(self,reset=True, input_folder="./indexation_2025/"):
        '''Indexation des documents'''
        if reset:
            self.create_table_candidates(input_folder, True)
        self.db.documents.drop()
        for candidate in self.db.candidats.find():
            self.index_document(candidate["filepath"])
            self.db.candidats.delete_one({"filepath":candidate["filepath"]})
        print(self.stats())

    def convert_to_csv_file(self, table_name) -> None:
        cursor = self.db[table_name].find()
        mongo_docs = list(cursor)
        docs = pandas.DataFrame(columns=[])
        for num, doc in enumerate(mongo_docs):
            doc["_id"] = str(doc["_id"])
            doc_id = doc["_id"]
            series_obj = pandas.Series(doc, name=doc_id)
            docs = docs.append(series_obj)
        docs.to_csv("{table_name}.csv", "\t")

    def preview_in_csv_format(self, table_name) -> None:
        cursor = self.db[table_name].find()
        mongo_docs = list(cursor)
        docs = pandas.DataFrame(columns=[])
        for num, doc in enumerate(mongo_docs):
            doc["_id"] = str(doc["_id"])
            # get document _id from dict
            doc_id = doc["_id"]
            # create a Series obj from the MongoDB dict
            series_obj = pandas.Series(doc, name=doc_id)
            # append the MongoDB Series obj to the DataFrame obj
            docs = docs.append(series_obj)
        # export MongoDB documents to CSV
        csv_export = docs.to_csv(sep=",")  # CSV delimited by commas
        print("\nCSV data:", csv_export)
    
    def export(self):
        self.export_OK()
        self.export_KO()
        self.export_CHOICE()

    def export_OK(self):
        with open("DOCS_OK.csv", "w") as f:
            row = "Ancien nom\tNouveau Nom\tN°Police\tN°Periode\tFamille de Prévoyance\tCategorie de Collège\tRaison Sociale"
            f.write(row+"\n")
            for doc in self.db.documents.find({"status": True}):
                r = "\t".join([doc["online_filepath"], doc["numper"]+".pdf", doc["police"]["_id"], doc["numper"], doc["fam"], doc["catcod"], doc["entrai"]])
                f.write(r+"\n")
    def export_KO(self):
        with open("DOCS_KO.csv", "w") as f:
            row = "Nom du fichier complet\tNom du document\tCommentaire"
            f.write(row+"\n")
            for doc in self.db.documents.find({"status": False, "nb_contrats": 0}):
                r = "\t".join([doc["online_filepath"], doc["filename"], doc["comment"]])
                f.write(r+"\n")

    def export_CHOICE(self):
        with open("CONTRATS_CHOIX.csv", "w") as f:
            row = "\t".join(["Ancien Nom", "N° de Police","N° de Contrat", "N° de Periode", "Catégorie de Collège", "Famille de risque", "Raison sociale"])
            f.write(row+"\n")
            for doc in self.db.documents.find({"status":False, "nb_contrats": {"$gt":1}}):
                for contrat in doc["contrats"]:
                    r = [doc.online_filepath, contrat["poledi"], contrat["polnum"], contrat["numper"], contrat["fam"], contrat["catcod"], contrat["entrai"]] 
                    f.write("\t".join(r)+'\n')
    
    
if __name__ == "__main__":
    db = DB("avenants_docs", False)
    db.index_documents(reset=True)
    db.export_OK()
    db.export_KO()
    db.export_CHOICE()