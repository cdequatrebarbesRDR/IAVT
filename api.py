#!/usr/bin/venv/python

import os
from fastapi import FastAPI
import aiofiles
from pydantic import BaseModel, FilePath, ValidationError
from database import DB
from models import Document
from models import Contrat
from fastapi import UploadFile



class CandidateModel(BaseModel):
    f: FilePath


app = FastAPI()

# @app.get("/")
# async def root():
#     return {"message": "Hello World"}

# @app.post("/documents/")
# async def get_document(candidate: CandidateModel):
#     d = Document(candidate.f)
#     return d.__export__() 

@app.get("/")
async def main():
    content = """
<body>
<form action="/files/" enctype="multipart/form-data" method="post">
<input name="files" type="file" multiple>
<input type="submit">
</form>
<form action="/uploadfiles/" enctype="multipart/form-data" method="post">
<input name="files" type="file" multiple>
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile):
    d = Document(file.filename)
    return d.__export__()

from typing import Annotated

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse

app = FastAPI()


@app.post("/uploadfiles/")
async def create_upload_files(
    files: Annotated[
        list[UploadFile], File(description="Multiple files as UploadFile")
    ],
):
    db = DB("contrat_doc", False)
    contrats = []
    for file in files:
        async with aiofiles.open(file.filename, 'wb') as out_file:
            content = await file.read()  # async read
            await out_file.write(content)  # async write

        d = db.index_document(file.filename)
        contrats.append(d.__export__())
    return {"contrats": contrats}


@app.get("/")
async def main():
    content = """
<body>
<h2> Selectionner des fichiers </h2>
<form action="/uploadfiles/" enctype="multipart/form-data" method="post">
<input name="files" type="file" multiple>
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)