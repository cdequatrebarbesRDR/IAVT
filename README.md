# AVENANTS ET CONTRATS

Chaque année, des avenants aux contrats sont émis par les Companie d'Assurance.
Ce sont des documents à intégrer dans la GED interne. 
Ils prennent la forme de PDF (scannés ou non), 
ils portent le numéro de contrat de référence et sont rangés par nom de companie.
IL s'agit de les renommer le numéro de la période correspondant au numéro de contrat.

## VERSION QUICK AND DIRTY: 1.10

Parser les nom de fichier et les documents pour récupérer le numéro du contrat 
puis requeter la Base XPRIME pour trouver le numéro du contrat et renvoyer le numéro de période.

| CIE.NAME | DANS LE NOM DE FICHIER | NUMERO DANS LE TEXTE | EXEMPLE NOM | EXEMPLE TEXTE | PATTERN TEXT| PATTERN N° |
|----------|----------------|----------------------|-------------|---------------|---------------|--------|
| HUMANIS  | Non|Oui| | |N°.\d{11,15}/s| \d{11,15}|
| AXA      |Oui| Oui |AXA_DECIBEL_FRANCE_2263898110400_1.pdf| 2263898110400[A-Z]| 2263898110400[A-Z]/d{2}|
| CNP      | Oui | Oui| CNP%20ASSURANCES_01012025___AVENANT___GRAVIERE_DU_RHIN___2530A.pdf|2530A| 2530A| \d{4}[A-Z]|
| UNIPREVOYANCE |Oui| Oui| UNIPREVOYANCE_A2P_COLMAR_4771300770000Z_SANTE.pdf| 4771300770000Z| 4771300770000Z|\d{13}[A-Z]|
| HENNER   |Non| Oui|||N°.\d{5}\s|\d{5}| 
| GROUPAMA |Non |Oui||N°.Contrat.:.\d{4}/\d{6}/\d{5}|\d{4}/\d{6}/\d{5}|
| MUTUELLE GENERALE|Oui |Oui| LMG_REV_STD_LAVT_PREV_MG_P_23394400MAP_FABRICATION_ET_MONTAGE_DE.pdf| MG/P/23394400MAP|MG/P/23394400MAP|r"([A-Z]{2}/[A-Z]{1,2}/\d{8}[A-Z]{3})/s"

## Requirements
- sys requirement:
  - ocrmypdf
  - mongo

- pip requirements
  - pymongo
  - fitz

## Code source

2 fichiers python
- models.py:
  - Contrat
  - Companie
  - Document
- database.py
  - Database
