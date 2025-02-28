# AVENANTS ET CONTRATS

Chaque année, des avenants aux contrats sont émis par les Companie d'Assurance.
Ce sont des documents à intégrer dans la GED interne. 
Ils prennent la forme de PDF (scannés ou non), 
ils portent le numéro de contrat de référence et sont rangés par nom de companie.
IL s'agit de les renommer le numéro de la période correspondant au numéro de contrat contenu dans la base XPRIME.

## VERSION QUICK AND DIRTY: 1.11

### Protocole

Parser les nom de fichier et les documents pour récupérer le numéro du contrat 
puis requeter la Base XPRIME pour trouver le numéro du contrat et renvoyer le numéro de période.

Renommer les fichiers avec le numéro de période et envoyer une copie dans le puits de données.

Le recollement (matching) est réalisé avec des règles définies par Compagnie: sur le nom du fichier ou le corsp du texte à l'aide d'expressions rationnelles. Une fois la réference isolée, une requête est faite sur la base de données qui contient tous les contrats en cours (sauf ALLIANZ) (cf. models.py Contrat).

Une table de correspondance a été construire pour normaliser les noms des Compagnies et faire correspondre la forme dans la Base de données extraite de EXPRIME et la forme de la Compagnie dans les dossiers d'indexation. (cf. models.py Compagnie)

Une fois les contrats indexés dans la base de données, le dossier Indexation est parcouru à la recherche de pdf.
Chaque document est classé dans un dossier par Compagnie (cf models.py Document) et parsé puis indexé dans la base de données dans une table Documents.

Une fois la compagnie détectée, le parsing s'effectue en fonction de la règle par Compagnie:
- si le numéro de référence est dans le nom du fichier:
  - on parse juste le numéro du fichier selon le motif défini au préalable
  - pour des besoins de vérification on a aussi noté le motif dans le corps du texte
- si le numéro de référence est dans le texte:
  - si le document est déjà OCRisé on parse le texte en fonction du motif défini au préalable
  - sinon on utilise OCRMYPDF et on parse le texte en fonction du motif défini au préalable 
(cf. database.py populate_documents_from_fs)

### Règles

| CIE.NAME | DANS LE NOM DE FICHIER | NUMERO DANS LE TEXTE | EXEMPLE NOM | EXEMPLE TEXTE | PATTERN TEXT| PATTERN N° |
|----------|----------------|----------------------|-------------|---------------|---------------|--------|
| HUMANIS  | Non|Oui| | |N°.\d{11,15}/s| \d{11,15}|
| AXA      |Oui| Oui |AXA_DECIBEL_FRANCE_2263898110400_1.pdf| 2263898110400[A-Z]| 2263898110400[A-Z]/d{2}|
| CNP      | Oui | Oui| CNP%20ASSURANCES_01012025___AVENANT___GRAVIERE_DU_RHIN___2530A.pdf|2530A| 2530A| \d{4}[A-Z]|
| UNIPREVOYANCE |Oui| Oui| UNIPREVOYANCE_A2P_COLMAR_4771300770000Z_SANTE.pdf| 4771300770000Z| 4771300770000Z|\d{13}[A-Z]|
| HENNER   |Non| Oui|||N°.\d{5}\s|\d{5}| 
| GROUPAMA |Non |Oui||N°.Contrat.:.\d{4}/\d{6}/\d{5}|\d{4}/\d{6}/\d{5}|
| MUTUELLE GENERALE|Oui |Oui| LMG_REV_STD_LAVT_PREV_MG_P_23394400MAP_FABRICATION_ET_MONTAGE_DE.pdf| MG/P/23394400MAP|MG/P/23394400MAP|r"([A-Z]{2}/[A-Z]{1,2}/\d{8}[A-Z]{3})/s"

### Requirements
- sys requirement:
  - ocrmypdf
  - mongo

- pip requirements
  - pymongo
  - fitz

### Code source

2 fichiers python
- models.py:
  - Contrat
  - Companie
  - Document
- database.py
  - Database

### Statistiques de performance

Un export en CSV
''' mongoexport --collection=documents --db=AVENANTS --type=csv --fields="cie.name,filepath,filename,input_filepath,ref,poledi,numper" --out="projects/avenants/STATS/PERF_MATCHING_v110.csv"
'''
La performance des règles est évaluée par compagnie pour eventuelles corrections des règles de detection
mais le score global de correspondance est de 77 % 
Un script bash est adjoint

## VERSION 2

L'objectif est de créer un service qui prend n'importe quel avenant à un contrat et renvoie:
1. La compagnie
2. Le numéro du contrat
3. La raison sociale de l'assuré
4. Le type de Assurance Santé/ Prévoyance
5. Le type de contrat
6. Le taux de cotisation

Maintenant qu'on a un set de données, étiqueté et classé, on va entrainer un modèle de classification automatique
On va commencer par essayer de lui faire deviner tout seul quelle est la compagnie. 
