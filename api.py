#!/usr/bin/venv/python
import os
from fastapi import FastAPI
from fastapi import UploadFile
from fastapi.responses import HTMLResponse
from fastapi.exceptions import HTTPException
import pydantic
import os
from typing import Optional, List

from fastapi import FastAPI, Body, HTTPException, status
from fastapi.responses import Response
from pydantic import ConfigDict, BaseModel, Field, FilePath
from pydantic.functional_validators import BeforeValidator

from typing_extensions import Annotated

import pymongo
#from bson import ObjectId
#import motor.motor_asyncio
from pymongo import ReturnDocument
from pymongo import MongoClient
PyObjectId = Annotated[str, BeforeValidator(str)]
from bson import ObjectId
# pydantic.json.ENCODERS_BY_TYPE[ObjectId]=str
from database import DB

app = FastAPI(
    title="Contrats API",
    summary="A sample API to access Contrats and Documents qualifications",
)
#client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGODB_URL"])
# client = MongoClient(os.environ["MONGODB_URL"])
client = DB("avenants_docs", False)
#db = db_engine.avenants_docs

class ContratModel(BaseModel):
    '''
    Container for a single Contrat Record
    '''
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    numper: str = Field(...)
    mut: str = Field(...)
    grp: str = Field(...)
    entnum: str = Field(...)
    entrai: str = Field(...)
    catcod: str = Field(...)
    prd: str = Field(...)
    opt: str = Field(...)
    fam: str = Field(...)
    polnum: str = Field(...)
    poledi: str = Field(...)
    datdeb: str = Field(...)
    datfin: str = Field(...)
    cienum: str = Field(...)
    cienom: str = Field(...)

class DocumentModel(BaseModel):
    '''
    Container for a single Document Record
    '''
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    ref: Optional[str] = Field(...)
    filepath: FilePath = Field(...)
    input_filepath: FilePath = Field(...)
    filename: str = Field(...)
    text: Optional[str] = Field(...)
    has_text: bool = Field(...)
    has_compagnie: bool = Field(...)
    online_filepath: str  = Field(...) 
    status: bool = Field(...)
    comment: Optional[str] = Field(...)
    contrats_nb: Optional[int] = Field(...)
    contrat: Optional[dict] = Field(...)
    police: Optional[str] = Field(...)
    numper: Optional[int] = Field(...)
    cie: Optional[dict] = Field(...)

class ContratCollection(BaseModel):
    """
    A container holding a list of `ContratModel` instances.

    This exists because providing a top-level array in a JSON response can be a [vulnerability](https://haacked.com/archive/2009/06/25/json-hijacking.aspx/)
    """

    contrats: List[ContratModel]

# from database import DB
from models import Document

#db_engine = DB("avenants_docs", False)
# INPUT_FOLDER = os.path.join(os.getcwd(), "/CANDIDATES/")

@app.get("/contrats/police/{police_id}",
response_description="Lister tous les contrats avec ce numéro de police",
response_model=ContratCollection,
response_model_by_alias=False,
)
def get_contrats_by_police(police_id: str):
    # return {"police_edition_nb": police_edition_nb}
    # contrats = db_engine.search_contrats_by_police_nb(police_edition_nb)
    # print(contrats)
    # return {"response": "ok"}
    # return {"count": len(contrats), "contrats": contrats}
    return ContratCollection(contrats=client.db.contrats.find({"$or":[{"polnum": {"$regex": police_id}}, {"poledi":{"$regex": police_id}}]}).to_list())

@app.get("/contrats/periode/{periode_nb}",
        response_description="Afficher le contrat avec ce numéro de période",
        response_model=ContratModel,
        response_model_by_alias=False,
)
def get_contrat_by_periode_number(periode_nb: str):
    contrat = client.db.contrats.find_one({"numper":periode_nb})
    if contrat is not None:
        return contrat
    raise HTTPException(status_code=404, detail=f"Aucun contrat avec le numéro de periode {periode_nb} n'a été trouvé")

@app.get("/contrats/raison_sociale/{raison_sociale}",
response_description="Lister tous les contrats avec cette raison sociale",
response_model=ContratCollection,
response_model_by_alias=False,
)
def get_contrats_by_raison_sociale(raison_sociale: str):
    return ContratCollection(contrats=client.db.contrats.find({"entrai": {"$regex": raison_sociale}}).to_list())

@app.get("/documents?filename={filename}",
response_description="Afficher les informations sur ce document",
response_model=DocumentModel,
response_model_by_alias=False         
)
def get_document_by_filename(filename: str):
    document = client.db.documents.find_one({"filename": filename})
    if document is not None:
        return document
    raise HTTPException(status_code=404, detail=f"Aucun document portant le nom {filename} n'a été trouvé")
    

@app.get("/documents/index/",
response_description="Indexer le document pour trouver le contrat associé",)
def main():
    content = """
<body>
<h2> Selectionner un fichier à indexer </h2>
<br><br>
<form action="/documents/" enctype="multipart/form-data" method="post">
<input name="file" type="file">
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)

@app.post("/documents/",
response_description="Indexer un nouveau document",
response_model=DocumentModel,
status_code=status.HTTP_201_CREATED,
response_model_by_alias=False,
)
def index_document(file: UploadFile):
    if len(file.filename) == 0:
        raise HTTPException(status_code=400, detail='Please provide a file')
    try:
        contents = file.file.read()
        with open(file.filename, 'wb') as f:
            f.write(contents)
            
    except Exception:
        raise HTTPException(status_code=500, detail='Something went wrong')
    finally:
        file.file.close()

    d = Document(file.filename)
    d.get_contrat()
    d.insert()
    document = client.db.documents.find_one({"filename": d.filename})
    if document is not None:
        return document
    raise HTTPException(status_code=500, detail='Something went wrong')

@app.put("/documents?filename={filename}")
def extract_document(filename: str):
    '''Extract more informations from document text'''
    pass

