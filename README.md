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


Attention verifier que le numéro de période pour un numéro de contrat est unique.
sinon recupérer la cat cod avec une règle de correspondance 
- AXA 738 doc avec multiples numper pour un police edition
- UNIPREVOYANCE verifier le numper pour une police d'edition
- 

Exclure les catcodes commencant:X,Z,K

NON CADRES SELON DISPOSITIF

NC 
GNC
NC1

KCAD  CADRES SELON DISPOSITIF
CAD CADRES SELON DISPOSITIF

['ASS', 'CAD', 'ENS', 'NC', 'NCO']

/avenants_input/AXA/TIR/AVT/AVT CP ANCR'EST_2263442630001_1 5687175.pdf >>> ['GNC', 'NC', 'NC1']



{
  'NC': "NON CADRES SELON DISPOSITIF"
  'ZSN': "NON CADRES SELON DISPOSITIF" ==> 5115792, AXA/AVT CP WOERNER GMBH_2712482001700_1 5115792.pdf ???
  "CAD": "CADRES SELON DISPOSITIF"
  'KCA': "CADRES SELON DISPOSITIF"
  'ENS': "ENSEMBLE DU PERSONNEL",
  "ASS": "ENSEMBLE DU PERSONNEL",
  "ZSE": "ENSEMBLE DU PERSONNEL/ DES SALARIES",
  "XAS": "ENSEMBLE DES CONGES SANS SOLDE REGIME ALSACE MOSELLE" "/ALSACE CONDITIONNEMENT_2845379410060_1
  'XAR': "RETRAITES"
  "XRC": "RETRAITES CADRES"
  'KCA': "CADRES SELON DISPOSITIF"
  "XVA" : "AYANT DROIT D UN ASSURE DECEDE POUR UNE DUREE DE 12 MOIS"
}

"MAL": "Santé"
"PRE": "Prévoyance"


[
  'ASS', 'CA1', 'CA3', 'CA4',
  'CAD', 'EMP', 'ENS', 'ETM',
  'KAS', 'KCA', 'KNC', 'MAI',
  'MJO', 'NC',  'NCE', 'RCA',
  'TNS', 'XAR', 'XAS', 'XCR',
  'XNP', 'XNR', 'XRC', 'XRN',
  'XVA', 'ZSA', 'ZSC', 'ZSE',
  'ZSN'
]

#### Pour HENNER en fait c'est la ganarite obsèque

aller chercher dans le texte les 3 numéro suivants N° (15519) si mention et sous gropupes
Nom du polnum dans le fichier
{
    _id: ObjectId('67b99a19721de3209f72d551'),
    numper: '500245292',
    mut: '53',
    grp: '',
    entnum: '5001101',
    entrai: 'OBSEQUES - DEPENDANCE - TELEASSISTANCE',
    catcod: 'ZR',
    prd: 'H-OBS 1504 45',
    opt: 'OPT',
    fam: 'OBS',
    polnum: 'C_GOA2',
    poledi: '15831/0',
    datdeb: '20150501',
    datfin: '99999999',
    cienum: '9',
    cienom: 'LA GARANTIE OBSEQUES',
    cie: {
      db_name: 'LA GARANTIE OBSEQUES',
      folder_name: null,
      name: null,
      id: 9
    }
  },

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
