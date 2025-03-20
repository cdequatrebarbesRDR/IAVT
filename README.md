# AVENANTS ET CONTRATS

Chaque année, des avenants aux contrats sont émis par les Companie d'Assurance qui les transmettent à Roederer par voie papier, mail et export Excel. Ce sont des documents à intégrer dans la GED interne pour mettre à jour les taux de cotisations des contrats et produits concernés. 

Pour les porteurs de risques, un assuré a un numéro de Contrat auquel Roederer ajoute un code de gestion interne (numéro de période, categorie de collège, famille de risque)
Les avenants aux contrats qu'il s'agit de réintégrer prennent la forme de PDF (scannés ou non), ils portent le numéro de contrat de référence et sont rangés par nom de companie dans un fichier Indexation.


## Procédures de recollement: Quick&Dirty

- Création d'une Base de données avec 4 tables:
  - TABLE "Contrats" qui contient tous les informations  pour un contrat tel que disponible dans l'export EXPRIME
  - TABLE "Compagnies" qui contient les différentes variations de la dénomination du porteur de risquess: nom dans la base de référence, nom du fichier, nom dans le texte et qui ramène à une dénomination normalisée ainsi que les formats attendus des numéros de contrats. 
  > Si la Compagnie n'est pas reconnue on applique un format de n° de contrat "standard"
  - TABLE "Candidats" qui contient tous les documents à indexer depuis le dossier souhaité
  - TABLE "Documents"" qui contient tous les documents qualifiés après indexation

- Scripts:
  - database.py
  - models.py
  - api.py


- 

### Règles par compagnie

| CIE.NAME | DANS LE NOM DE FICHIER | NUMERO DANS LE TEXTE | EXEMPLE NOM | EXEMPLE TEXTE | PATTERN TEXT| PATTERN N° |
|----------|----------------|----------------------|-------------|---------------|---------------|--------|
| HUMANIS  | Non|Oui| | |N°.\d{11,15}/s| \d{11,15}|
| AXA      |Oui| Oui |AXA_DECIBEL_FRANCE_2263898110400_1.pdf| 2263898110400[A-Z]| 2263898110400[A-Z]/d{2}|
| CNP      | Oui | Oui| CNP%20ASSURANCES_01012025___AVENANT___GRAVIERE_DU_RHIN___2530A.pdf|2530A| 2530A| \d{4}[A-Z]|
| UNIPREVOYANCE |Oui| Oui| UNIPREVOYANCE_A2P_COLMAR_4771300770000Z_SANTE.pdf| 4771300770000Z| 4771300770000Z|\d{13}[A-Z]|
| HENNER   |Non| Oui|||N°.\d{5}\s|\d{5}| 
| GROUPAMA |Non |Oui||N°.Contrat.:.\d{4}/\d{6}/\d{5}|\d{4}/\d{6}/\d{5}|
| MUTUELLE GENERALE|Oui |Oui| LMG_REV_STD_LAVT_PREV_MG_P_23394400MAP_FABRICATION_ET_MONTAGE_DE.pdf| MG/P/23394400MAP|MG/P/23394400MAP|r"([A-Z]{2}/[A-Z]{1,2}/\d{8}[A-Z]{3})/s"|


### Resultats

CANDIDATS = 3511
OK = 1331
KO = 2065
DOUBLONS SIMPLES = 60
CHOIX DU NUMERO DE PERIODES = 55 

### Limites

- La référence du contrat trouve souvent plusieurs numéro de polices dans la base: de 2 à 222
-  
- 
- Soucis dans la base de données de référence: numéro de contrats 

La procédure 

### Suggestion d'amélioration


