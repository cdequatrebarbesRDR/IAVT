#!/usr/bin/env/python
# Contrat Companie Document
import re
import os
import shutil
import difflib

import pdfplumber
import fitz
import subprocess as sp
import spacy

COMPANIE_FOLDER_CIENOM = {
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
class Companie:
    def __init__(self):
        '''une compagnie peut être qualifiée par un nom de dossier ou un nom dans la base'''
        self.name = None
        self.id =  None
        self.folder = None

    def build_from_db(self, row:dict):
        self.name(row["CIENOM"])
        self.id(int(row["CIENUM"]))
        
    
    def set_name(self, cienom:str):
        '''nom tel que dans la base'''
        self.name = cienom
        self.slug() 
        return self
    
    def slug(self):
        '''version simplifiée du nom dans la base'''
        self.slug = re.sub(self.name.lower().upper(), "(\\.|-|_)", "").split(" ")[0]
        return self.slug
    
    def set_id(self,cienum:int):
        '''numero de companie dans la base'''
        self.id = cienum 
        return self.id
    
    def set_folder(self, folder):
        '''nom tel que dans le fichier'''
        self.folder = folder
        return self.folder
    
    def matching_ref(self, input: str):
        '''reconciliation entre le nom et le dossier'''
        if self.name is not None and self.folder is None:
            if input in self.slug:
                self.folder = input
        
class Contrat:
    def __init__(self, cell: dict):
        '''Les propriétés du contrat sont telles que dans la base'''
        for k, v in cell.items():
            setattr(self, k.lower(), v)
    def __str__(self):
        print(f"Contrat n°({self.polnum} // Millesime n° {self.poledi} // Compagnie {self.cienom} // RAISON SOCIALE {self.entraid}")

class Document:
    def __init__(self, filepath: str):
        chunks = filepath.split('/')
        self.input_dir = chunks[0] 
        for cie_dir,cie_name  in COMPANIE_FOLDER_CIENOM.items():
            if cie_dir in filepath:
                c = Companie()
                c.set_folder(cie_dir)
                c.set_name(cie_name)
                self.cie = c.__dict__
                break
        self.filename = chunks[-1]
        self.filepath = os.path.join(self.input_dir, self.filename)
          
        
    @property
    def pdf(self):
        '''OCR only if not scanned'''
        TMP_file = os.path.join(self.input_dir, "TMP_"+ self.filename)
        output = sp.getoutput(f"ocrmypdf {self.filepath} {TMP_file}")
        if not re.search("PriorOcrFoundError: page already has text!",output):
            # shutil.copyfile(TMP_file, new_filepath)
            os.rename(TMP_file, self.filepath)  
        else:
            os.remove(TMP_file)
    
    @property
    def text(self):
        with fitz.open(self.filepath) as doc:
            text = ""
            for page in doc:
                text += page.get_text().strip()
            if len(text) != "":
                return text
            return None
    
    @property
    def token(self):
        return re.split(r'/s', self.text)
    
    def __str__(self):
        return f"Document ({self.filename})"

    def search_by_polinum(self, polinum:str):
        return
     
    def search_ref(self, candidate: str):
        '''detect REF numbers in filename or in document text'''
        ref_polnum = re.compile("(?P<ref>[A-Z]*?/d+([A-Z]*?))")
        m = re.findall("([0-9]+)([A-Z]+)?", candidate, re.MULTILINE)
        print(m)
        
    def search_pattern(self, candidate: str, pattern: str):
        # difflib.get_close_matches(pattern, candidate, n=3, cutoff=0.6)
        m = re.findall(pattern, candidate, re.MULTILINE)
        print(m)
        #s = difflib.SequenceMatcher(lambda x: x == " ",candidate,pattern)
        # print(round(s.ratio(), 3))
    
    # def set_compagnie(self, filepath):
    #     os.path.split(filepath)[1]

if __name__ == "__main__":
    # d = Document("./avenants_output/7_UNION NAT CULTURE_2204200082900_1.pdf")
    # # print(d.text)
    # # d.search_ref(d.text)
    # print(d.search_pattern(d.text, "2204200082900"))
    # print(d.search_nbs())
    # d2 = Document('./avenants_output/40_AUTOMOBILE CLUB MOSELLE_4935004740000A_SANTE.pdf')
    d2 = Document('./avenants_output/22_2B AUTOMOTIVE - CONTRATS_20241118_001667_118.pdf')
    print(d2.text)
    print(d2)
    d3 = Document('./avenants_output/54_REV_STD_LAVT_PREV_MG-P-23394400MAP_FABRICATION_ET_MONTAGE_DE.pdf')
    print(d3)
    print(d3.pdf)