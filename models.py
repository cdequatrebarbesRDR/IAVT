#!/usr/bin/env/python
# Contrat compagnie Document
import re
import os
import shutil
#import difflib

# import pdfplumber
import fitz
import subprocess as sp
# impor♣t spacy
#from database import DB

COMPAGNIE_NOM_RULES = {
    'AXA': "AXA",
    'Groupama Gan Vie': "GROUPAMA",
    'La Garantie Obsèques': "HENNER",
    'HENNER': 'HENNER',
    "MALAKOFF HUMANIS": "HUMANIS",
    "QUATREM": "HUMANIS",
    "uniprevoyance": "UNIPREVOYANCE",
    "La Mutuelle Générale": "MUTUELLE GENERALE",
    "LA MUTUELLE GENERALE": "MUTUELLE GENERALE",
    "CNP Assurances": "CNP"


}
COMPAGNIE_FOLDER_CIENOM = {
    'AZ': 'ALLIANZ',
    "AXA": "AXA",
    "APICIL": "APICIL",
    # "CNP ASSURANCES": "CNP",
    "CNP": "CNP",
    "GAN": "GROUPAMA",
    "GENERALI": "GENERALI",
    "GROUPAMA": "GROUPAMA",
    "HENNER": "HENNER",
    "LMG":"MUTUELLE GENERALE",
    "MH": "HUMANIS",
    "UNIPREVOYANCE": "UNIPREVOYANCE",
    "LA GARANTIE OBSEQUES": 'La Garantie Obsèques' 
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
    "MUTUELLE GENERALE": "MUTUELLE GENERALE",
    'MG - MUTUELLE GENERALE': "MUTUELLE GENERALE", 
    'UNIPREVOYANCE': "UNIPREVOYANCE",
    "LA GARANTIE OBSEQUES": "HENNER"
}
CIENOM_COMPAGNIE_DB = {v: k for k, v in COMPAGNIE_DB_CIENOM.items()}
CIENOM_COMPAGNIE_FOLDER = {v: k for k, v in COMPAGNIE_FOLDER_CIENOM.items()}
class Compagnie:
    def __init__(self):
        '''une compagnie peut être qualifiée par un nom de dossier ou un nom dans la base
        on lui donne un nom normalisé entre les deux
        attention: toutes les assurances dans la base n'ont pas forcément de dossier
        pour l'indexation
        '''
        self.db_name = None
        self.folder_name = None 
        self.name = None
        self.id =  None
        self.search_in_fn= True
        self.search_in_txt = False
        self.pattern_txt = r"(N|n)°(?P<ref>.*?\d{4}.*?)\s"
        
        
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
                #CNP ASSURANCES_01012025___AVENANT___GRAVIERE_DU_RHIN___2530A 
                self.pattern_fn = re.compile(r"_/d{4,5}[A-Z]\.pdf") 
                self.pattern_txt = re.compile(r"(?P<ref>\d*[A-Z])/s")
                #self.fn_ref = filename.split("_")[-1]
            elif self.name in ["MUTUELLE GENERALE"]:
                #LMG_REV_STD_LAVT_PREV_MG_P_23394400MAP_FABRICATION_ET_MONTAGE_DE.pdf
                self.pattern_fn = re.compile(r"_[A-Z]{2}_[A-Z]{1,2}_\d{8}[A-Z]{3}_")
                #self.pattern_fn = re.compile(r"_[A-Z]*_[A-Z]_\d*[A-Z]*_")
                self.pattern_txt = re.compile(r"(?P<ref>[A-Z]*\/[A-Z]\/\d*[A-Z]*)/s")
                
                
            else:
                raise NotImplementedError(f"Cie {self.name} rule extraction is not implemented")
        elif self.name in ["ALLIANZ"]:
            self.search_in_fn = False
            self.pattern_fn = None
            self.search_in_txt = True
            self.pattern_txt = re.compile(r"\s.*?(?P<ref>\d{4,5}.*?000)\s")
            raise NotImplementedError(f"Cie {self.name} should not be indexed")
        
        elif self.name in ["GROUPAMA", "HENNER", "HUMANIS", "LA GARANTIE OBSEQUES"]:
            #HENNER  N°.\d{5}\s
            #GROUPAMA N°.Contrat.:.\d/{4}/\d{6}/\d{5}
            #HUMANIS N°.\d{6}.*?\(Offre .*?\)
            self.search_in_fn = False
            self.pattern_fn = None
            self.search_in_txt = True
            if self.name == "GROUPAMA":
                #GROUPAMA N°.Contrat.:.\d/{4}/\d{6}/\d{5}
                self.pattern_txt = re.compile(r"N°.Contrat.*?(?P<ref>\d*\/\d*\/\d*)")
            elif self.name in ["HENNER", "LA GARANTIE OBSEQUES"]:
                #HENNER  N°.\d{5}\s
                # N° 15746/115
                #self.pattern_txt = re.compile(r"(N°.*?)?\s(ST)?(?P<ref>\d{5}(\d{3})?.*?\s)")
                self.pattern_txt = re.compile(r"N°.*?(?P<ref>\d{5}(\d{3})?.*?\s).")
            elif self.name == "HUMANIS":
                 #HUMANIS N°.\d{6}.*?\(Offre .*?\)
                self.pattern_txt = re.compile(r"[n|N]°.*?(?P<ref>\d{11,15}).*?\s")
            else:
                self.pattern_txt = re.compile(r"\s.*?(?P<ref>\d*.*?)\s")
        return self

   
class Contrat:
    def __init__(self, cell: dict):
        '''Les propriétés du contrat sont telles que dans la base'''
        for k, v in cell.items():
            setattr(self, k.lower(), v)
        
        self.cie = Compagnie()
        self.cie.build_from_db(cell)
        
    def __str__(self):
        print(f"Contrat n°({self.polnum} // Millesime n° {self.poledi} // Compagnie {self.c["name"]} // RAISON SOCIALE {self.entraid}")
        return f"Contrat n°({self.polnum} // Millesime n° {self.poledi} // Compagnie {self.c["name"]} // RAISON SOCIALE {self.entraid}"
    
    def __document__(self):
        '''export to mongodb'''
        self.dict_xport = {k: v for k,v in self.__dict__.items() if "cie" not in k}
        self.dict_xport["cie"] = {"name": self.cie.name}
        return self.dict_xport
        

class Document:
    def __init__(self, *args, **kwargs):
        self.numper = None
        self.poledi = None
        self.ref = None
        self.polnum = None
        self.entrai = None
        self.entnum = None
        self.fam = None
        self.matches = {"count": 0, "poledis": [], "numpers": [], "catcods": [], "fams": []}
        if isinstance(args[0],str):
            self.build_from_path(args[0])
        elif isinstance(args[0], dict):
            self.build_from_db(args[0])
        elif "filepath" in kwargs:
            self.build_from_path(kwargs["filepath"])
        elif "record" in kwargs:
            self.build_from_db(kwargs["record"])
            
    def build_from_path(self, filepath:str):
        self.found = False
        self.ref = None
        self.match = None
        self.text = None
        self.entnum = None
        self.entrai = None
        self.normalize_filename(filepath)
        self.scan()
        self.get_compagnie()
        self.get_ref() 
    
    def build_from_db(self, db_doc: dict):
        for k,v in db_doc.items():
            if k == "cie":
                c = Compagnie()
                c.build_from_name(v["name"])
                c.get_rules()
                setattr(self, "cie",c)
            else: 
                setattr(self, k,v)
        self.get_ref()
        return self


    
    def normalize_filename(self, filepath, output_dir="CANDIDATES"):
        '''Slugify filename and write text into doc .txt'''
        self.input_filepath = filepath
        chunks = filepath.split('/')
        filename = chunks[-1]
        self.output_dir = os.path.join(os.getcwd(), output_dir)
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.filename = re.sub(r"[&|\(|\)|']", "", re.sub(r"[\s+|-]", "_", filename))
        self.filepath = os.path.join(self.output_dir, self.filename)
        self.filetxt = os.path.join(self.output_dir, self.filename.replace(".pdf", ".txt"))
        if not os.path.exists(self.filepath):
            shutil.copy(filepath, self.filepath)    
        self.scan()
        with open(self.filetxt, "w") as f:
            f.write(self.text)
            
        return self
    def scan(self):
        if self.text is None:
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
                self.text = re.sub(r"\s\s", ' ', " ".join(pages)).strip()        
        return self.text
    
    def get_compagnie(self):
        if self.text is None:
            self.scan()
        '''Rechercher le nom de la Compagnie d'Assurance dans le texte'''
        for cie_name, cie_rules in COMPAGNIE_NOM_RULES.items():
            c = Compagnie()
            cie_match = re.search(cie_name, self.text)
            if cie_match is not None:
                c.build_from_name(cie_rules)
                c.get_rules()
                self.cie = c
                return self.cie
        # print(f"Compagnie name not found in doc text : {self.filepath}.\n Trying with folder name")
        # self.cie = {"name": "", "folder": ""}
        # Method using folder HINT
        for cie_dir,cie_name  in COMPAGNIE_FOLDER_CIENOM.items():
            
            if "/"+cie_dir in self.filepath:
                c = Compagnie()
                c.build_from_folder(cie_dir)
                c.get_rules()
                # print(re.search(c.name, self.text))
                # cast to dict to insert in mongo
                self.cie = c
                return self.cie
        self.cie = Compagnie()
        self.cie.name = "Unknown"
        return self
     
    def get_ref(self):
        if self.cie.search_in_fn:
            self.search_ref_in_filename()
            return self.validate_ref_in_text()
        self.match = True
        return self.search_ref_in_text()
    
      
    def search_ref_in_filename(self):
        '''Some of the documents have a ref number in the filename: used as a hint'''
        chunks_filename = self.filename.replace(".pdf", "").split("_")
        if self.cie.name == "CNP":
            self.found = True
            self.ref = chunks_filename[-1]
            return (self.found, self.ref)
        if self.cie.name in ["UNIPREVOYANCE", "AXA"]:
            #ADIL_67_4360135480000V_SANTE.pdf
            filtered_chunks = [n for n in chunks_filename if len(n) >= 13]
            if len(filtered_chunks) == 0:
                self.ref = None
            elif len(filtered_chunks) == 1:
                self.ref = filtered_chunks[0]
                self.found = True
            else:
                filtered_chunks = [n for n in filtered_chunks if n[0:11].isdigit()]
                if len(filtered_chunks) == 1:
                    self.ref = filtered_chunks[0]
                    self.found = True
                else:
                    self.ref = None

            return (self.found, self.ref)
                    
        if self.cie.name == "MUTUELLE GENERALE":
            #LMG_REV_STD_LAVT_SANTE_NR_PMSS_MG_S_20235500PAN_SACOR.pdf
            chunks_fn = self.filename.split("_")
            pos_ref =  [i for i,n in enumerate(chunks_filename) if n[:7].isdigit()]
            if len(pos_ref) != 0:
                pos_ref = pos_ref[0]
                self.ref = "/".join(chunks_fn[pos_ref-2: pos_ref+1])
                self.found = True
            return (self.found, self.ref)
            
        return (self.found, self.ref)
    
    def search_ref_in_text(self):
        '''Search contrat nb in text given a pattern'''
        m = re.search(self.cie.pattern_txt, self.text)
        if m is None:
            return (self.match, self.ref)
        self.match = True
        self.ref = m.group("ref").strip()
        if self.cie.name == "GROUPAMA":
            self.ref = "/".join(self.ref.split("/")[0:2])
        return (self.match, self.ref)
        
    def validate_ref_in_text(self):
        '''Search complete contrat nb in text given a ref pattern'''
        self.match = False
        if self.cie.name == "UNIPREVOYANCE":
            self._target = re.compile(fr"(?P<ref2>[A-Z]\d*)\s\/.*?(?P<ref>{self.ref})")
            match = list(set(["".join([m[1], m[0]]) for m in re.findall(self._target, self.text) if m is not None]))
            if len(match) > 0:
                self.original_ref = self.ref
                self.ref = match[0]
                self.match = True
                return (self.match, self.ref)  
            
                           
        if self.cie.name == "AXA":
            self._target = re.compile(fr"{self.ref}((?P<ref2>[A-Z])|\s\/[A-Z]*\s(?P<ref3>\d*))\s")
            match = list(set([m for m in re.findall(self._target, self.text) if m is not None ]))
            if len(match) == 2:
                self.match = True   
                additional_ref = sorted([[n for n in m_group if n != ''][-1] for m_group in match], reverse=True)
                self.ref = self.ref+ "".join(additional_ref)
                return (self.match, self.ref)
            
            
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
                    self.match = True
                    self.ref = re.sub(r"\s", "", match[0])
                    return (self.match, self.ref)
                else:
                    self.match = True
                    self.ref = re.sub(r"\s", "", " ".join(match))
                    return (self.match, self.ref)
        return self.search_ref_in_text()

    def __str__(self):
        return f"Document ({self.filename})"
    def __export__(self):
        '''Compatible with Mongo Import'''
        self.dict_xport = {k: v for k,v in self.__dict__.items() if "cie" not in k and not k.startswith("_")}
        self.dict_xport["cie"] = {"name": self.cie.name}
        # print(self.dict_xport)
        return self.dict_xport
    
    def store(self, reset=False, dbname = "AVENANTS_CONTRATS"):
        from database import DB
        db = DB(dbname)
        if reset:
            db.db["documents"].insert_one(self.__export__())
            return self
        record = db.db.documents.find_one({"filename": self.filename}, {"_id":1})
        if record is not None:
            db.db["documents"].update_one({"_id": record["_id"]}, {"$set":self.__export__()}, True)
        else:
            db.db["documents"].insert_one(self.__export__())
        return self
    
    def match_contrat(self, dbname = "AVENANTS_CONTRATS"):
        from database import DB
        db = DB(dbname, False)
        if self.ref is None:
            self.status = False
            self.message = "KO: detection du numéro de contrat"
            return self
        self.status, contrat = db.search_contrat(self.ref, self.cie.name)
        if self.status:
            
            for k,v in contrat.items():
                if k != "_id":
                    setattr(self,k,v)
            self.new_filename = f"{self.numper}.pdf"
        else:
            self.new_filename = ""
        return self

    
if __name__ == "__main__":
    from database import DB
    db = DB("AVENANTS_CONTRATS", False)
    print(db.documents.count_documents({}))
    docs = db.documents.find()
    for doc in docs:
        d = Document(doc["input_filepath"])
        d.match_contrat()
        # row = [d.input_filepath.replace("./avenants_input/", "S://Contrat\1 - INDEXATIONS/indexation_2025/"), str(d.numper), str(d.ref), str(d.polnum),  str(d.poledi),";".join(d.matches["numpers"]), ";".join(d.matches["catcods"]), str(d.entrai), str(d.entnum)]
        # print("\t".join(row))
        # if d2.found: 
        #     print(d2.ref, d2.matches)
    
        
    #print(d2.find_contrat())
    # assert (d1.cie.name == "CNP", d1.cie.name)
    # assert (d1.ref == "2529Z", d1.ref)
    # d2 = Document("./avenants_input/AXA/TIR/AVT/AFCE FORMATION_2275781410000_1.pdf")
    # assert (d2.cie.name == "AXA")
    # assert (d2.ref == "2275781410000Z50", d2.ref)
    # d3 = Document('./avenants_input/MH/TIR/CONTRATS_20241118_001662_11.pdf')
    # assert (d3.cie.name == "HUMANIS")
    # assert (d3.ref == None, d3.ref)
    # d4 = Document("./avenants_input/UNIPREVOYANCE/TIR/AVT/ADAX'O INTERNATIONAL_4360136160000B_SANTE.pdf")
    # assert (d4.cie.name == "UNIPREVOYANCE")
    # assert (d4.ref == "4360136160000BF2417", d4.ref)
    # d5= Document("./avenants_input/LMG/AVT/REV_STD_LAVT_SANTE_R_PMSS_MG-S-2012275S_TREVNAS_FLUVIAL.pdf")
    # assert (d5.cie.name == "MUTUELLE GENERALE")
    # assert (d5.ref == "MG/S/2012275S", d5.ref)
   