#!/usr/bin/venv/python

import os
from fastapi import FastAPI
import aiofiles
from pydantic import BaseModel, FilePath, ValidationError
from database import DB
from models import Document
from models import Contrat
from fastapi import UploadFile
from typing import Annotated
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse



class CandidateModel(BaseModel):
    f: FilePath


app = FastAPI()



@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile):
    d = Document(file.filename)
    return d.__export__()


app = FastAPI()


@app.post("/uploadfiles/")
async def create_upload_files(
    files: Annotated[
        list[UploadFile], File(description="Multiple files as UploadFile")
    ],
):
    db = DB("contrat_docs", False)
    contrats = []
    for file in files:
        async with aiofiles.open(file.filename, 'wb') as out_file:
            content = await file.read()  # async read
            await out_file.write(content)  # async write

        d = db.index_document(file.filename)
        contrats.append(d.__export__())
    return {"contrats": contrats}


@app.get("/index/")
async def main():
    content = """
<body>
<h2> Selectionner des fichiers </h2>
<br><br>
<form action="/uploadfiles/" enctype="multipart/form-data" method="post">
<input name="files" type="file" multiple>
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)
@app.get("/avenants/")
def choose_police():
    db = DB("avenant_doc", False)
    return {"documents":db.documents.find({"status": True})}



@app.get("/polices/")
def choose_police():
    db = DB("avenant_doc", False)
    return {"polices":db.documents.find({"comment": {"$regex": "police"}})}


@app.get("/periodes/")
def choose_police():
    db = DB("avenant_doc", False)
    return {"polices":db.documents.find({"comment": {"$regex": "police"}})}

