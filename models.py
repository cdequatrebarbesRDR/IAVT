#!/usr/bin/env/python
# Contrat compagnie Document
import re
import os
import shutil
#import difflib

# import pdfplumber
import fitz
import subprocess as sp
# import spacy

COMPAGNIE_FOLDER_CIENOM = {
    'AZ': 'ALLIANZ',
    "AXA": "AXA",
    "APICIL": "APICIL",
    "CNP": "CNP",
    "GAN": "GROUPAMA",
    "GENERALI": "GENERALI",
    "GROUPAMA": "GROUPAMA",
    "HENNER": "HENNER",
    "LMG":"MUTUELLE GENERALE",
    "MH": "HUMANIS",
    "UNIPREVOYANCE": "UNIPREVOYANCE"
}

COMPAGNIE_DB_CIENOM = {
    'APICIL PREVOYANCE': "APICIL", 
    'AXA COURTAGE': "AXA", 
    'AXA TNS': "AXA", 
    'C.N.P.': "CNP", 
    'GENERALI COLLECTIVES': "GENERALI", 
    'GENERALI FRANCE PGM-PPL': "GENERALI", 
    'GGE - GROUPAMA GRAND EST': "GROUPAMA", 
    'GROUPAMA GAN VIE': "GROUPAMA", 
    'HARMONIE MUTUALITE': "HUMANIS", 
    'HENNER': "HENNER", 
    'HUMANIS': 'HUMANIS',  
    'HUMANIS (EX NOVALIS)': 'HUMANIS', 
    'HUMANIS (ex  APRIONIS)': 'HUMANIS', 
    'MALAKOFF HUMANIS COURTAGE': 'HUMANIS', 
    'MALAKOFF/URRPIMMEC' : 'HUMANIS', 
    'MG - MUTUELLE GENERALE': "MUTUELLE GENERALE", 
    'MGD':  "MUTUELLE GENERALE",
    'MGEN': "MUTUELLE GENERALE",
    'UNIPREVOYANCE': "UNIPREVOYANCE"
}
CIENOM_COMPAGNIE_DB = {v: k for k, v in COMPAGNIE_DB_CIENOM.items()}
CIENOM_COMPAGNIE_FOLDER = {v: k for k, v in COMPAGNIE_FOLDER_CIENOM.items()}
class Compagnie:
    def __init__(self):
        '''une compagnie peut être qualifiée par un nom de dossier ou un nom dans la base
        on lui donne un nom normalisé entre les deux
        attention: toutes les assurances dans la base n'ont pas forcement de dossier
        '''
        self.db_name = None
        self.folder_name = None
        self.name = None
        self.id =  None
        self.search_in_fn= True
        self.search_in_txt = False
        
        
    def build_from_db(self, row:dict):
        self.db_name = row["CIENOM"]
        self.id = int(row["CIENUM"])
        try:
            self.name = COMPAGNIE_DB_CIENOM[self.db_name]
            self.folder_name = CIENOM_COMPAGNIE_FOLDER[self.name]
        except KeyError:
            if self.folder_name is not None:
                self.name = COMPAGNIE_FOLDER_CIENOM[self.folder_name]
            pass
        self.get_rules()

    def build_from_folder(self, folder):
        self.folder_name = folder
        self.name = COMPAGNIE_FOLDER_CIENOM[folder]
        self.db_name = CIENOM_COMPAGNIE_FOLDER[self.name]
        self.get_rules()
    def build_from_name(self, name):
        self.name = name
        self.get_rules()

    def get_rules(self):
        '''Given NAME get matching rules and pattern'''
        if self.name in ["UNIPREVOYANCE", "AXA", "MUTUELLE GENERALE", "CNP"]:
            self.search_in_fn= True
            self.search_in_txt = False
            self.pattern_txt = ""
            if self.name in ["AXA","UNIPREVOYANCE"]:
                if self.name == "AXA":
                    self.pattern_fn = re.compile(r"_\d{13}(.*?)_")
                    self.pattern_txt = re.compile(r"N°.*?(?P<ref>\d{13}.*?)\s")
                else:
                    self.pattern_fn = re.compile(r"_\d{13}[A-Z]_")
                    self.pattern_txt = re.compile(r"(?P<ref>\d{13}[A-Z])")

                #"AXA_DECIBEL_FRANCE_2263898110400_1.pdf"
                #UNIPREVOYANCE_A2P_COLMAR_4771300770000Z_SANTE.pdf
                
            elif self.name in ["CNP"]:
                #CNP%20ASSURANCES_01012025___AVENANT___GRAVIERE_DU_RHIN___2530A 
                self.pattern_fn = re.compile(r"_/d{4,5}[A-Z]\.pdf") 
                self.pattern_txt = re.compile(r"(?P<ref>\d*[A-Z])/s")
                #self.fn_ref = filename.split("_")[-1]
            elif self.name in ["MUTUELLE GENERALE"]:
                self.pattern_fn = re.compile(r"_[A-Z]{2}_[A-Z]{1,2}_\d{8}[A-Z]{3}_")
                #self.pattern_fn = re.compile(r"_[A-Z]*_[A-Z]_\d*[A-Z]*_")
                self.pattern_txt = re.compile(r"(?P<ref>[A-Z]*\/[A-Z]\/\d*[A-Z]*)/s")
                #LMG_REV_STD_LAVT_PREV_MG_P_23394400MAP_FABRICATION_ET_MONTAGE_DE.pdf
                
            else:
                raise NotImplementedError(f"Cie {self.name} rule extraction is not implemented")
        elif self.name in ["ALLIANZ"]:
            self.search_in_fn = False
            self.pattern_fn = None
            self.search_in_txt = True
            self.pattern_txt = re.compile(r"\s.*?(?P<ref>\d{4,5}.*?000)\s")
            raise NotImplementedError(f"Cie {self.name} should not be indexed")
        
        elif self.name in ["GROUPAMA", "HENNER", "HUMANIS"]:
            #HENNER  N°.\d{5}\s
            #GROUPAMA N°.Contrat.:.\d/{4}/\d{6}/\d{5}
            #HUMANIS N°.\d{6}.*?\(Offre .*?\)
            self.search_in_fn = False
            self.pattern_fn = None
            self.search_in_txt = True
            if self.name == "GROUPAMA":
                #GROUPAMA N°.Contrat.:.\d/{4}/\d{6}/\d{5}
                self.pattern_txt = re.compile(r"N°.Contrat.*?(?P<ref>\d*\/\d*\/\d*)")
            elif self.name == "HENNER":
                self.pattern_txt = re.compile(r"N°.*?(?P<ref>\d{5}.*?).")
            elif self.name == "HUMANIS":
                self.pattern_txt = re.compile(r"[n|N]°.*?(?P<ref>\d{11,15}).*?\s")
            else:
                self.pattern_txt = re.compile(r"\s.*?(?P<ref>\d{4,5}.*?)\s")
        return self

   
class Contrat:
    def __init__(self, cell: dict):
        '''Les propriétés du contrat sont telles que dans la base'''
        for k, v in cell.items():
            setattr(self, k.lower(), v)
        
        c = Compagnie()
        c.build_from_db(cell)
    
    def __str__(self):
        print(f"Contrat n°({self.polnum} // Millesime n° {self.poledi} // Compagnie {self.c["name"]} // RAISON SOCIALE {self.entraid}")
        return f"Contrat n°({self.polnum} // Millesime n° {self.poledi} // Compagnie {self.c["name"]} // RAISON SOCIALE {self.entraid}"
    def __document__(self):
        '''export to mongodb'''
        self.dict_xport = {k: v for k,v in self.__dict__.items() if "cie" not in k}
        self.dict_xport["cie"] = {"name": self.cie.name}
        return self.dict_xport
        

class Document:
    def __init__(self, filepath: str):
        self.found = False
        self.poledi = None
        self.polnum = None
        self.entrai = None
        self.numper = None
        self.normalize_filename(filepath)
        self.get_compagnie(filepath)
        # retourner le numéro de contrat
        self.get_match()
        # A vérifier dans la base
        
    def get_compagnie(self, filepath):
        # self.cie = {"name": "", "folder": ""}
        for cie_dir,cie_name  in COMPAGNIE_FOLDER_CIENOM.items():
            if "/"+cie_dir+"/" in filepath:
                c = Compagnie()
                c.build_from_folder(cie_dir)
                c.get_rules()
                # cast to dict to insert in mongo
                self.cie = c
                return self.cie
        self.cie = Compagnie()
        return self.cie

    def normalize_filename(self, filepath):
        self.input_filepath = filepath
        chunks = filepath.split('/')
        filename = chunks[-1]
        output_dir = os.path.join(os.getcwd(), "DOCS")
        self.filename = re.sub(r"[&|\(|\)|']", "", re.sub(r"[\s+|-]", "_", filename))
        self.filepath = os.path.join(output_dir, self.filename)
        if not os.path.exists(self.filepath):
            shutil.copy(filepath, self.filepath)    
        return self
    
    def get_match(self):
        self.ref = None
        if self.cie.search_in_fn:
            self.ref = self.search_ref_in_filename()
            if self.ref is not None:
                return self.ref
        return self.search_ref_in_text()
    def scan(self):
        self.get_text()
        if self.text is None:
            self.ocr_pdf()
        return self.text
    def ocr_pdf(self):
        '''OCR only if not scanned'''
        # output_file = os.path.join(self.output_dir, "TMP_"+self.filename)
        cmd = f"ocrmypdf {self.filepath} {self.filepath}"
        output = sp.getoutput(cmd)
        
        # print("OUTPUT OCR:", output)
        return self.get_text()
        
    def get_text(self):
        self.text = None
        pages = []
        with fitz.open(self.filepath) as doc:
            self.text = ""
            for page in doc:
                content = page.get_text().strip()
                if content != '':
                    pages.append(page.get_text().strip())
                # self.text += page.get_text().strip()
            if len(pages) == 0:
                self.text = None
            else:
                self.text = re.sub('\n', ' ', "\n".join(pages)).strip()        
        return self.text
      
    def search_ref_in_filename(self):
        if self.cie.name in  ["UNIPREVOYANCE", "AXA"]:
            #re.search in filename

            chunks_filename =  [n for n in self.filename.split("_") if n.isdigit() and len(n)>= 13]
            print(chunks_filename)
            if len(chunks_filename) == 0:
                chunks_filename =  [n for n in self.filename.split("_") if n.isdigit()]
                if len(chunks_filename) == 1:
                    self.ref = chunks_filename[0]
                return self.ref
            if len(chunks_filename) == 1 :
                self.ref = chunks_filename[0]
                return self.ref
            else:
                print("MULTIPLE REFS", self.cie.name, "NOT FOUND", chunks_filename, self.filename)
                return self.ref
            
        if self.cie.name == "MUTUELLE GENERALE":
            #LMG_REV_STD_LAVT_SANTE_NR_PMSS_MG_S_20235500PAN_SACOR.pdf
            chunks_fn = self.filename.split("_")
            pos_ref =  [i for i,n in enumerate(chunks_fn) if n[:7].isdigit()]
            if len(pos_ref) != 0:
                pos_ref = pos_ref[0]
                self.ref = "/".join(chunks_fn[pos_ref-2: pos_ref+1])
                return self.ref
            
        if self.cie.name == "CNP":
            self.ref = self.filename.split("_")[-1].replace(".pdf", "")
            return self.ref
        return self.ref
    
    def search_ref_in_text(self):
        self.scan()
        m = re.search(self.cie.pattern_txt, self.text)
        if m is None:
            return self.ref
        self.ref = m.group("ref").strip()
        if self.cie.name == "GROUPAMA":
            self.ref = "/".join(self.ref.split("/")[0:2])
        return self.ref
    
    def __str__(self):
        return f"Document ({self.filename})"
    def __document__(self):
        '''Compatible with Mongo Import'''
        self.dict_xport = {k: v for k,v in self.__dict__.items() if "cie" not in k}
        self.dict_xport["cie"] = {"name": self.cie.name}
        return self.dict_xport
        
    
    
if __name__ == "__main__":
    pass
    # d3 = Document('./avenants_input/MH/TIR/CONTRATS_20241118_001662_11.pdf')
    # assert d3.cie["name"] == "HUMANIS"
    # d3.match("")
    # print(d3.target)
    # d3= Document("./avenants_input/AXA/TIR/AVT/KO/DECIBEL FRANCE_2263898110400_1.pdf")
    # d3.match("")
    # print(d3.target)