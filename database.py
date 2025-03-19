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
    
    def search_polnum(self, ref:str)->List[int, list|None]:
        polenum_nb = self.db.polnums.count_documents({"_id": {"$regex": ref}})
        if polenum_nb == 0:
            return (polenum_nb, None)
        elif polenum_nb  == 1:
            polenum_item = self.db.polnums.find_one({"_id": {"$regex": ref}})
            return (polenum_nb, [polenum_item])
        else:
            polenum_items = self.db.polnums.find({"_id": {"$regex": ref}})
            return (polenum_nb, list(polenum_items))
    
    def get_contrat_by_police_nb(self, police_nb:str)-> Contrat| None:
        _contrats = self.get_contrats_by_police_nb(police_nb)
        if len(_contrats) != 1:
            return None
        contrats = self.get_contrats_by_period_nb(_contrats[0].numper)
        if len(contrats) != 1:
            return None
        return contrats[0]
    
    def get_contrats_by_police_nb(self, police_nb:str)-> List[Contrat]:
        contrats = []
        for c in self.db.contrats.find({"$or":[{"$polnum": {"$regex": police_nb}}, {"$poledi": {"$regex": police_nb}}]}):
            contrats.append(c)
        return contrats
    
    def get_contrats_by_period_nb(self, period_nb:str)-> List[Contrat]:
        contrats = []
        for c in self.db.contrats.find({"$numper": period_nb}):
            contrats.append(c)
        return contrats
    
    def search_police(self, ref:str)->List[int, list|None, list|None]:
        '''search by poledi and then by polnum
        return poledis
        return periodes (numper,cacod, fam)
        '''
        poledi_nb, poledis = self.search_poledi(ref)
        if poledi_nb == 1:
            return (poledi_nb, poledis, poledis[0]["periodes"])
        elif poledi_nb == 0:
            polnum_nb, polnums = self.search_polnum(ref)
            if polnum_nb == 1:
                return (polnum_nb, polnums, polnums[0]["periodes"])
            elif polnum_nb == 0:
                return (0, None, None)
            periodes = []
            for p in polnums:
                periodes.extend(p["periodes"])
            return (polnum_nb, polnums,  periodes)
        periodes = []
        for p in poledis:
            periodes.extend(p["periodes"])
        return (poledi_nb, poledis, periodes)
    
    def select_periode(self, periodes:list)-> List[int,list|None,list|None ]:
        '''Sélectionner un numéro de période'''
        if periodes is None:
            return 0, None, "Aucun numéro de période: KO"
        periodes_nb = len(periodes)
        if periodes_nb == 0:
            return 0, None, "Aucun numéro de période: KO" 
        if len(periodes) == 1:
            return periodes_nb,[periodes[0]], "Un seul numéro de période: OK"
        #deduplicate
        return self.filter_periodes_by_catcod(periodes)
                 
        
        
    def filter_periodes_by_catcod(self, periodes):
        '''
        Filtrer les périodes:
        - fam,catcod uniques
        - valides: pas de catcod commençant K Z X
        - generiques: les catcods les plus generiques ['ENS', 'ASS', 'CAD','NC', 'CA*' ]
        '''
        uniq_periodes = self.get_numpers_by_unique_catcodes(periodes)
        print(uniq_periodes)
        if len(uniq_periodes) == 1:
            return len(uniq_periodes), uniq_periodes.values()[0], "Doublons: Choisir le numéro de période "  
        #filter using blacklist
        valid_periodes = self.filter_numpers_by_catcode_blacklist(uniq_periodes)
        if len(valid_periodes) == 0:
            return len(valid_periodes), periodes, "Aucune catégorie valide:  Choisir le numéro de période"
        if len(valid_periodes) == 1:
            return len(valid_periodes), valid_periodes, "Une seule catégorie valide: OK"
        #filter using whitelist
        generic_periodes = self.filter_numpers_by_catcode_whitelist(valid_periodes)
        if len(generic_periodes) == 0:
            return len(generic_periodes), valid_periodes, "Aucune catégorie générique: Choisir le numéro de période"
        if len(generic_periodes) == 1:
            return len(generic_periodes), generic_periodes, "Une seule catégorie valide et générique: OK"
        else:
            return len(generic_periodes), generic_periodes, "Plusieurs catégories générique : Choisir le numéro de période"
        
        
    def filter_numpers_by_catcode_blacklist(self, numpers, blacklist=["Z", "X", "K"])-> list:
        '''blacklist: X, K, Z'''
        filtered_numpers = []
        for k, numper_list in numpers.items():
            if k[1][0] not in blacklist:
                filtered_numpers.extend(numper_list)
        return filtered_numpers
    
    def filter_numpers_by_catcode_whitelist(self, numpers, prioritylist=["ASS", "ENS", "CAD", "NC", "ETM", "TNS"])-> list:
        '''whitelist: priority code from choose genereic over specific'''
        return [n for n in numpers if n["catcod"] in prioritylist]
    
    def get_numpers_by_unique_catcodes(self, numpers)-> dict:
        '''
        deduplicate numpers by dict(catcode,fam)
        catcods = {(fam, catcode)= [numper]}
        '''
        catcods = defaultdict.fromkeys([(n["fam"], n["catcod"]) for n in numpers], [])
        for key in catcods:
            catcods[key] = [n for n in numpers if key == (n["fam"], n["catcod"])]
        return catcods
    def order_by_fam_and_catcod(contrats):
    
    
    def get_contrats_by_police(self,police_nb:str)-> List[Contrat]:
        '''Récupérer l'ensemble des contrats à partir d'un numéro de police'''
        contrats = []
        for c in self.db.contrats.find({"$or": [{"$poledi": {"$regex": police_nb}},{"$polnum": {"$regex": police_nb}}]}):
            if "cie" in c.keys():
                print(c)
            #     c["cienom"] = c.cie["name"]
            c = Contrat(c)
            contrats.append(c)
        return contrats


    def get_contrats_by_periode(self, numper: str)->List[Contrat]:
        '''Récupérer l'ensemble des contrat à partir d'un numéro de periode'''
        contrats = []
        for c in self.db.contrats.find({"numper": numper}):
            if "cie" in c.keys():
                print(c)
            #     c["cienom"] = c.cie["name"]
            c = Contrat(c)
            contrats.append(c)
        return contrats
    
        

    def get_contrat(self, police_nb:str) -> Union[Contrat| None]:
        polices = self.get_contrats_by_police(police_nb)
        if len(polices) == 1:
            periodes = self.get_contrats_by_periode(polices[0].numper)
            
            if len(periodes) == 1:
                return periodes[0]
            if len(periodes) > 1:
                self.filter_contrats_by_catcod(periodes)
    

    def filter_contrats_by_catcod(periodes:List[Contrat]):
         

    def index_document(self, filepath)-> Document:
        '''Indexer un document en le reliant à un contrat'''
        d = Document(filepath)
        d.online_filepath = d.filepath.replace("./", "S://Contrat/1 - INDEXATIONS/")
        d.status = False
        if d.ref is not None:
            d.police = None
            d.poledis = None
            d.numper = None
            police_nb, d.polices, d.periodes = self.search_police(d.ref)
            
            if police_nb == 0:
                d.comment = "Aucun numéro de police trouvé"
                return d
            elif police_nb == 1:
                d.police = d.polices[0]
                numpers_nb, d.numpers, d.comment = self.select_periode(d.periodes)
                if numpers_nb == 0:
                    d.comment = "Aucun numéro de période trouvé"
                    
                    return d
                if numpers_nb == 1:
                    # d.comment = "Un seul numéro de période trouvé"
                    d.numper = d.numpers[0]["numper"]
                    d.catcod = d.numpers[0]["catcod"]
                    d.fam = d.numpers[0]["fam"]
                    d.status = True
                    return d
                elif numpers_nb > 1:
                    d.numper = [x["numper"] for x in d.numpers]
                    d.catcod = [x["catcod"] for x in d.numpers]
                    d.fam = [x["fam"] for x in d.numpers]
                    d.status = False
                    return d
                return d
            else:
                numpers_nb, d.numpers, d.comment = self.select_periode(d.periodes)
                d.comment = "Choisir le numéro de police"
                if numpers_nb == 0:
                    
                    d.numper = None
                    d.catcod = None
                    d.fam = None

                    return d
                if numpers_nb == 1:
                    #d.comment = "Un seul numéro de période trouvé"
                    #d.police = d.numpers[0]["poledi"]
                    d.numper = d.numpers[0]["numper"]
                    d.catcod = d.numpers[0]["catcod"]
                    d.fam = d.numpers[0]["fam"]
                    d.status = False
                    return d
                elif numpers_nb > 1:
                    
                    d.numper = [x["numper"] for x in d.numpers]
                    d.catcod = [x["catcod"] for x in d.numpers]
                    d.fam = [x["fam"] for x in d.numpers]
                    d.status = False
                    return d
                
            return d 
        d.comment = "Aucune référence détectée"
        return d 
        
    def index_documents(self,reset=True, input_folder="./indexation_2025/"):
        '''Indexation des documents'''
        if reset:
            self.create_table_candidates(input_folder, True)
        self.db.documents.drop()
        for candidate in self.db.candidats.find():
            
            doc = self.index_document(candidate["filepath"])
            # print(doc.filepath, doc.cie.name, doc.status, doc.comment)
            document = doc.__export__()
            self.db.documents.insert_one(document)
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
        self.export_ko()
        self.export_doublons()
        self.export_numper_choice()

    def export_OK(self):
        with open("NUMPERS_OK.csv", "w") as f:
            row = "Ancien Nom\tNouveau Nom\tN°Police\tN°Periode\tFamille de Prévoyance\tCategorie de Collège"
            f.write(row+"\n")
            for doc in self.db.documents.find({"status": True}):
                new_name = doc["online_filepath"].replace(doc["online_filepath"].split("/")[-1], f"{doc["numper"]}.pdf")
                self.db.documents.update_one({"_id": doc["_id"]}, {"$set":{"new_filename": new_name}})
                r = "\t".join([doc["online_filepath"], new_name, doc["police"]["_id"], doc["numper"], doc["fam"], doc["catcod"]])
                f.write(r+"\n")

    def export_numper_choice(self):
        with open("NUMPERS_CHOIX.csv", "w") as f:
            row = "\t".join(["Ancien Nom", "N° de Police","N° de Contrat"]+["N° de Periode", "Catégorie de Collège", "Famille de risque", "Raison sociale"]*8)+"\n"
            f.write(row+"\n")
            for doc in self.db.documents.find({"comment":{"$regex":"catégories générique"}}):
                
                r = [doc["online_filepath"], doc["periodes"][0]["poledi"], doc["periodes"][0]["polnum"]] + ["","", "",""]*8
                nb_periodes = len(doc["periodes"])
                periodes_flat = []
                for p in doc["periodes"]:
                    periodes_flat.extend([p["numper"], p["catcod"], p["fam"], p["entrai"]])
                r[2:nb_periodes] = periodes_flat 
                
                f.write("\t".join(r)+'\n')
    
    def export_ko(self):
        with open("NUMPERS_KO.csv", "w") as f:
            row = "\t".join(["Ancien Nom","N° de Police", "Numéro de période"])+"\n"
            f.write(row+"\n")
            for doc in self.db.documents.find({"$or":[{"comment":{"$regex":"Choisir le numéro de police"}}, {"comment": {"$regex": "Aucun numéro de période trouvé" }},{"comment": {"$regex": "Aucun numéro de police trouvé" }}, {"comment": {"$regex": "Aucune référence" }}]}):
                r = "\t".join([doc["online_filepath"], "", ""])+"\n"
                f.write(r)

    def export_doublons(self):
        with open("NUMPERS_DOUBLONS.csv", "w") as f:
            row = "\t".join(["Ancien Nom", "N° de Police"]+["N°de Contrat","N° de Periode", "Catégorie de Collège", "Famille de risque", "Raison sociale"]*8)+"\n"
            f.write(row+"\n")
            for doc in self.db.documents.find({"comment":{"$regex":"Doublons"}}):
                r = [doc["online_filepath"], doc["periodes"][0]["poledi"]] + ["","", "","",""]*8
                nb_periodes = len(doc["periodes"])+1
                periodes_flat = []
                for p in doc["periodes"]:
                    periodes_flat.extend([p["polnum"], p["numper"], p["catcod"], p["fam"], p["entrai"]])
                r[2:nb_periodes] = periodes_flat 
                print(r)
                f.write("\t".join(r)+'\n')

if __name__ == "__main__":
    db = DB("avenant_docs", False)
    # db.index_documents(reset=True)
    db.export_OK()
    db.export_numper_choice()
    db.export_doublons()
    db.export_ko()