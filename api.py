#!/usr/bin/venv/python

from database import DB
from models import Document
from models import Contrat

from fastapi import FastAPI

db = DB("AVENANTS_CONTRATS", False)

app = FastAPI()

@app.get("/documents/{document_path}")
def get_new_filename(document_path:str):
    d = Document(document_path)
    return 

@app.get("/contrats/")
def get_numpers():
    return