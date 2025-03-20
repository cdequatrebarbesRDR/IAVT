#!/usr/bin/env/python
# Contrat compagnie Document
import re
import os
import shutil
from typing import List, Union
#import difflib

# import pdfplumber
import fitz
import subprocess as sp

#NOM in TEXT OR FOLDER NAME OR DB NAME => NORMALIZED NAME
COMPAGNIE_RULES = {
    'AXA': "AXA",
    'Groupama Gan Vie': "GROUPAMA",
    'La Garantie Obsèques': "HENNER",
    'HENNER': 'HENNER',
    "MALAKOFF HUMANIS": "HUMANIS",
    "QUATREM": "HUMANIS",
    "uniprevoyance": "UNIPREVOYANCE",
    "La Mutuelle Générale": "MUTUELLE GENERALE",
    "LA MUTUELLE GENERALE": "MUTUELLE GENERALE",
    "CNP Assurances": "CNP",
    'AZ': 'ALLIANZ',
    "AXA": "AXA",
    "APICIL": "APICIL",
    "CNP ASSURANCES": "CNP",
    "CNP": "CNP",
    "GAN": "GROUPAMA",
    "GENERALI": "GENERALI",
    "GROUPAMA": "GROUPAMA",
    "Groupama": "GROUPAMA",
    "HENNER": "HENNER",
    "LMG":"MUTUELLE GENERALE",
    "MH": "HUMANIS",
    "UNIPREVOYANCE": "UNIPREVOYANCE",
    "LA GARANTIE OBSEQUES": "HENNER", 
    'La Garantie Obsèques': "HENNER",
    'APICIL PREVOYANCE': "APICIL", 
    'AXA COURTAGE': "AXA", 
    'AXA TNS': "AXA", 
    "MH": "HUMANIS",
    "GAN": "GRPOUPAMA",
    "CNP": "CNP",
    "CNP ASSURANCES": "CNP",
    'C.N.P.': "CNP", 
    'GENERALI COLLECTIVES': "GENERALI", 
    'GENERALI FRANCE PGM-PPL': "GENERALI", 
    'GGE - GROUPAMA GRAND EST': "GROUPAMA",
    'GAN': "GAN", 
    "GROUPAMA": "GROUPAMA",
    'GROUPAMA GAN VIE': "GROUPAMA",
    'Groupama Gan Vie': "GROUPAMA", 
    'HARMONIE MUTUALITE': "HUMANIS", 
    'HENNER': "HENNER", 
    'HUMANIS': 'HUMANIS', 
    "Humanis": "HUMANIS", 
    'HUMANIS (EX NOVALIS)': 'HUMANIS', 
    'HUMANIS (ex  APRIONIS)': 'HUMANIS', 
    'MALAKOFF HUMANIS COURTAGE': 'HUMANIS', 
    'MALAKOFF/URRPIMMEC' : 'HUMANIS', 
    "MUTUELLE GENERALE": "MUTUELLE GENERALE",
    'MG - MUTUELLE GENERALE': "MUTUELLE GENERALE", 
    'UNIPREVOYANCE': "UNIPREVOYANCE",
    "LA GARANTIE OBSEQUES": "HENNER"
}

COMPAGNIE_RULES_NAME = {k: [] for k in COMPAGNIE_RULES.values()}
for k, v in COMPAGNIE_RULES.items():
    COMPAGNIE_RULES_NAME[v].append(k)


class Compagnie:
    def __init__(self, slug):
        '''une compagnie est identifiée avec un slug qui peut être:
        - le nom du dossier
        - le nom dans le document
        - le nom dans la base de contrat
        on le ramène à une version normalisée pour lui rattacher les règles
        attention: toutes les assurances dans la base n'ont pas forcément de dossier
        pour l'indexation et pas forcément de règles définies
        '''
        self.slug = slug
        self.search_in_fn = False
        self.search_in_txt = True
        self.get_rules()
    
    @property
    def name(self):
        if self.slug is None:
            return None
        try:
            return COMPAGNIE_RULES[self.slug]    
        except KeyError:
            return None
    
    def search_name(self, text):
        for key, value in COMPAGNIE_RULES.items():
            if re.search(key, text) is not None:
                self.slug = key
                self.name = value
                self.get_rules()
                return
        raise NotImplementedError
             
    def get_rules(self):
        '''Given NAME get matching rules and pattern'''
        self.poledi_fn = None
        #generic contrat NB
        self.poledi_txt = r"\s(?P<ref>.*?\d{3,}.*?)\s"
        self.search_in_fn = False
        self.search_in_txt = True
        if self.name is None:
            self._pattern_fn = None
            self._pattern_txt = re.compile(self.poledi_txt)
            return self
        if self.name in ["UNIPREVOYANCE", "AXA", "MUTUELLE GENERALE", "CNP"]:
            self.search_in_fn = True
            self.search_in_txt = False
            self.poledi_txt = r"\s.*?\d{3,}.*?\s"
            self.poledi_fn = r"_.*?\d{3,}.*?[_|\.]"

            if self.name in ["AXA","UNIPREVOYANCE"]:
                if self.name == "AXA":
                    #"AXA_DECIBEL_FRANCE_2263898110400_1.pdf"
                    self.poledi_fn = r"_\d{13}(.*?)_"
                    self.poledi_txt = r"N°.*?(?P<ref>\d{13}.*?)\s"
                    
                else:
                    #UNIPREVOYANCE_A2P_COLMAR_4771300770000Z_SANTE.pdf
                    self.poledi_fn = r"_\d{13}[A-Z]_"
                    self.poledi_txt = r"(?P<ref>\d{13}[A-Z])"
                
                
                
            elif self.name in ["CNP"]:
                #CNP ASSURANCES_01012025___AVENANT___GRAVIERE_DU_RHIN___2530A 
                self.poledi_fn = r"_/d{4,5}[A-Z]\.pdf"
                self.poledi_txt = r"(?P<ref>\d*[A-Z])/s"
                #self.fn_ref = filename.split("_")[-1]
            elif self.name in ["MUTUELLE GENERALE"]:
                #LMG_REV_STD_LAVT_PREV_MG_P_23394400MAP_FABRICATION_ET_MONTAGE_DE.pdf
                self.poledi_fn = r"_[A-Z]{2}_[A-Z]{1,2}_\d{8}[A-Z]{3}_"
                #self.pattern_fn = re.compile(r"_[A-Z]*_[A-Z]_\d*[A-Z]*_")
                self.poledi_txt = r"(?P<ref>[A-Z]*\/[A-Z]\/\d*[A-Z]*)/s"
            
           
        elif self.name in ["ALLIANZ"]:
            self.search_in_fn = False
            self.poledi_fn = None
            self.search_in_txt = True
            self.poledi_txt = r"\s.*?(?P<ref>\d{4,5}.*?000)\s"
            
            
        elif self.name in ["GROUPAMA", "HENNER", "HUMANIS", "LA GARANTIE OBSEQUES"]:
            self.search_in_fn = False
            self.poledi_fn = None
            self.search_in_txt = True
            if self.name == "GROUPAMA":
                #GROUPAMA N°.Contrat.:.\d/{4}/\d{6}/\d{5}
                self.poledi_txt = r"N°.Contrat.*?(?P<ref>\d*\/\d*\/\d*)"
            elif self.name in ["HENNER", "LA GARANTIE OBSEQUES"]:
                #HENNER  N°.\d{5}\s
                # N° 15746/115
                #self.pattern_txt = re.compile(r"(N°.*?)?\s(ST)?(?P<ref>\d{5}(\d{3})?.*?\s)")
                self.poledi_txt = r"N°.*?(?P<ref>\d{5}(\d{3})?.*?\s)."
            elif self.name == "HUMANIS":
                 #HUMANIS N°.\d{6}.*?\(Offre .*?\)
                self.poledi_txt = r"[n|N]°.*?(?P<ref>\d{11,15}).*?\s"
         
            
        if self.poledi_fn is not None and self.search_in_fn:
            #NB: on ne stocke pas la version compilée dans la BDD
            self._pattern_fn = re.compile(self.poledi_fn)
        self._pattern_txt = re.compile(self.poledi_txt)
        return self
       
        
    def __export__(self):
        '''export to mongodb'''
        return {k: v for k,v in self.__dict__.items() if not k.startswith("_")}
        
class Contrat:
    def __init__(self, cell: None):
        '''Les propriétés du contrat sont telles que dans l'export CSV'''
       
        for k, v in cell.items():
            if not k.startswith("_") or "_id" not in k:
                setattr(self,k.lower(), v)
        
    
            
    @property
    def compagnie(self)-> Compagnie:
        if hasattr(self, "cienom"):
            return Compagnie(self.cienom)
        return Compagnie(None)
        
    
    def __export__(self):
        '''export to mongodb'''
        return {k: v for k,v in self.__dict__.items() if k != "compagnie"}
        
        

class Document:
    def __init__(self, filepath):
        self.ref = None
        self.normalize_filename(filepath)
        self.get_text()
        self.get_compagnie()
        self.get_ref()
        
    def normalize_filename(self, filepath:str):
        '''Slugify filename and write text into doc .txt'''
        self.filepath = filepath
        self.input_filepath = filepath
        chunks = filepath.split('/')
        filename = chunks[-1]
        self.filename = re.sub(r"[&|\(|\)|']", "", re.sub(r"[\s+|-]", "_", filename))
        return self.filename
    
    def ocr(self):
        '''OCR using ocrmypdf and transform directly the pdf'''
        cmd = f"ocrmypdf --output-type pdf {self.input_filepath} {self.filepath}"
        sp.getoutput(cmd)
        return 
            
    def get_text(self)->str:
        '''extract text using fitz'''
        self.ocr()
        pages = []
        with fitz.open(self.filepath) as doc:
            self.text = ""
            for page in doc:
                content = page.get_text().strip()
                if content != '':
                    pages.append(page.get_text().strip())
        if len(pages) == 0:
            self.text = None
        else:
            self.text = re.sub(r"\s\s", ' ', " ".join(pages)).strip()        
        self.has_text = self.text is not None
        return self.text
            
    def get_compagnie(self)-> Compagnie:
        '''Rechercher le nom de la Compagnie d'Assurance dans le texte'''
        self.has_compagnie = False
        if self.has_text:
            for cie_name, cie_rules in COMPAGNIE_RULES.items():    
                cie_match = re.search(cie_name, self.text)
                if cie_match is not None:
                    self.cie = Compagnie(cie_name)
                    self.has_compagnie = True
                    return self.cie
            print(f"Compagnie name not found in doc text: {self.filepath}")
            # Method using folder HINT in case failed to detect
            for cie_dir,cie_name  in COMPAGNIE_RULES.items():
                if cie_dir in self.filepath:
                    self.cie = Compagnie(cie_dir)
                    self.has_compagnie = True
                    return self.cie
            self.has_compagnie = False
            self.cie = Compagnie(None)
            return self.cie
     
    def get_ref(self)-> Union[str|None]:
        self.ref = None
        if self.has_text and self.has_compagnie:
            if self.cie.search_in_fn:
                self.search_ref_in_filename()
                self.validate_ref_in_text()
                if self.valid_ref is False:
                    return self.search_ref_in_text()    
            return self.search_ref_in_text()
        return self.ref 
    
      
    def search_ref_in_filename(self):
        '''Some of the documents have a ref number in the filename: used as a hint'''
        self.has_ref = False
        self.ref = None
        chunks_filename = self.filename.replace(".pdf", "").split("_")
        if self.cie.name == "CNP":
            self.has_ref = True
            self.ref = chunks_filename[-1]
            return self
        if self.cie.name in ["UNIPREVOYANCE", "AXA"]:
            #ADIL_67_4360135480000V_SANTE.pdf
            filtered_chunks = [n for n in chunks_filename if len(n) >= 13]
            if len(filtered_chunks) == 0:
                return self
            elif len(filtered_chunks) == 1:
                self.ref = filtered_chunks[0]
                self.has_ref = True
                return self
            else:
                filtered_chunks = [n for n in filtered_chunks if n[0:11].isdigit()]
                if len(filtered_chunks) == 1:
                    self.ref = filtered_chunks[0]
                    self.has_ref = True
                    return self
                
            return self
                    
        if self.cie.name == "MUTUELLE GENERALE":
            #LMG_REV_STD_LAVT_SANTE_NR_PMSS_MG_S_20235500PAN_SACOR.pdf
            chunks_fn = self.filename.split("_")
            pos_ref =  [i for i,n in enumerate(chunks_filename) if n[:7].isdigit()]
            if len(pos_ref) != 0:
                pos_ref = pos_ref[0]
                self.ref = "/".join(chunks_fn[pos_ref-2: pos_ref+1])
                self.has_ref = True
                return self
        return self
    
    def search_ref_in_text(self):
        '''Search contrat nb in text given a pattern'''
        self.has_ref = False
        if self.text is None:
            return self
        if hasattr(self, "cie"):
            ref_pattern = self.cie._pattern_txt
        else:
            ref_pattern = re.compile(r"\s(P?<ref).*?\d{4,}.*?\s")
        m = re.search(ref_pattern, self.text)
        if m is None: 
            return self.has_ref
        else:
            self.has_ref = True
            self.ref = m.group("ref").strip()
        if self.cie.name == "GROUPAMA":
            self.has_ref = True
            self.ref = "/".join(self.ref.split("/")[0:2])
        return self
        
    def validate_ref_in_text(self):
        '''Search complete contrat nb in text given a ref pattern hint '''
        self.valid_ref = False
        if not hasattr(self, "ref"):
            self.ref = None
            return self.valid_ref
        if self.cie.name == "UNIPREVOYANCE":
            self._target = re.compile(fr"(?P<ref2>[A-Z]\d*)\s\/.*?(?P<ref>{self.ref})")
            match = list(set(["".join([m[1], m[0]]) for m in re.findall(self._target, self.text) if m is not None]))
            if len(match) > 0:
                self.original_ref = self.ref
                self.ref = match[0]
                self.valid_ref = True
            return self.valid_ref  
            
                           
        if self.cie.name == "AXA":
            self._target = re.compile(fr"{self.ref}((?P<ref2>[A-Z])|\s\/[A-Z]*\s(?P<ref3>\d*))\s")
            match = list(set([m for m in re.findall(self._target, self.text) if m is not None ]))
            if len(match) == 2:
                self.valid_ref = True   
                additional_ref = sorted([[n for n in m_group if n != ''][-1] for m_group in match], reverse=True)
                self.ref = self.ref+ "".join(additional_ref)
            return self.valid_ref
            
            
        if self.cie.name in ["CNP", "MUTUELLE GENERALE"]:
            if self.cie.name == "CNP":
                self._target = re.compile(fr"(?P<ref2>{self.ref})")
                
            
            if self.cie.name == "MUTUELLE GENERALE":
                # print("SELF.REF", self.ref)
                regex_ref = self.ref.split("/")
                self._target = re.compile(fr"(?P<ref2>{regex_ref[0]}.*?{regex_ref[1]}.*?{regex_ref[2]})")
                
            match = list(set([m for m in re.findall(self._target, self.text) if m is not None]))
            if len(match) > 0:
                if len(match) == 1:
                    self.valid_ref = True
                    self.ref = re.sub(r"\s", "", match[0])
                    
                else:
                    self.valid_ref = True
                    self.ref = re.sub(r"\s", "", " ".join(match))
                return self.valid_ref
        return self.valid_ref
    def get_contrats(self, DB_NAME="avenant_docs"):
        # méthode d'indexation unitaire a partir d'une reference
        # qui appelle celle de DB 
        # import dans la fonction pour éviter les boucles d'import circulaire
        from database import DB
        db = DB(DB_NAME)
        self.contrats = None
        if self.ref is not None:
            return db.get_contrats(self.ref)
        return None
    
    def get_contrat(self, DB_NAME="avenants_docs"):
        # méthode d'indexation unitaire a partir d'une reference
        # qui appelle celle de DB 
        # import dans la fonction pour éviter les boucles d'import circulaire
        from database import DB
        db = DB(DB_NAME)
        if self.ref is not None:
            self.found, contrats = db.get_contrat_by_police_nb(self.ref)
            if self.found:
                self.contrat = contrats
                self.new_filename = self.contrat.numper
                return self.__export__()
            else:
                if len(contrats) > 1:
                    self.contrats = contrats
                    return self.__export__()
        else:
            self.found = False
            self.comment = f"reference non detectée"
            return self.__export__()

    def __str__(self):
        return f"Document ({self.filename})"
    def __export__(self):
        '''Compatible for mongo'''
        self._dict_xport = {k: v for k,v in self.__dict__.items() if "cie" not in k and not k.startswith("_")}
        if hasattr(self, "cie") and self.cie is not None:
            self._dict_xport["cie"] = {"name": self.cie.name}
        else:
            self._dict_xport["cie"] = None
        return self._dict_xport
    
if __name__ == "__main__":
    pass