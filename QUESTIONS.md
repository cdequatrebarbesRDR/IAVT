- stats db
- nb doc
- nb ref
- nb poledis
- nb poledi
- nb numpers
- nb numper unique
- catcode

# CHOIX du NUMERO DE POLICE 
Cas Henner tous les numéro de contrats en cours?
 
# CHOIX DU NUMPER

Cas n°0: un seul numper, le catcode est parfois non discrimant

Cas n°1: aucun des numper n'a une bonne catégorie
- H_COMM_2507759780001_1.pdf ['6054526', '6054529'] ['KCA', 'ZSC'] 
>>> Obseques
- AXA KAP_33_2512704051001_1.pdf ['5248385', '5248427'] ['XNX', 'ZSN']

{
  _id: ObjectId('67ce048400bcc51dcd46f607'),
  numper: '5248385',
  mut: '10',
  grp: '',
  entnum: '286826',
  entrai: 'KAPP 33',
  catcod: 'ZSN',
  prd: 'ADA-BET.SURCO 3',
  opt: 'G R',
  fam: 'MAL',
  polnum: '2512704051001H95',
  poledi: '2512704051001H95',
  datdeb: '20161001',
  datfin: '99999999',
  cie: { name: 'AXA' }
},
{
  _id: ObjectId('67ce048400bcc51dcd46f609'),
  numper: '5248427',
  mut: '10',
  grp: '',
  entnum: '286826',
  entrai: 'KAPP 33',
  catcod: 'XNX',
  prd: 'ADA-BET+CJT-SU3',
  opt: 'OPT',
  fam: 'MAL',
  polnum: '2512704051001H95',
  poledi: '2512704051001H95',
  datdeb: '20161001',
  datfin: '99999999',
  cie: { name: 'AXA' }
}
]

Cas n°2 Plusieurs fois le meme catcode pour plusieurs numper

AFCE_FORMATION_2275781400000_1.pdf ['8079859', '8079874', '8079915', '8079930'] ['CAD', 'CAD', 'KCA', 'KCA']
{
    _id: ObjectId('67ce048300bcc51dcd46ef64'),
    numper: '8079915',
    mut: '2',
    grp: '',
    entnum: '273993',
    entrai: 'AFCE FORMATION SAS',
    catcod: 'KCA',
    prd: 'AXA-AFCE CAD R.',
    opt: 'G R',
    fam: 'MAL',
    polnum: '2275781400000E19',
    poledi: '2275781400000E19',
    datdeb: '20220101',
    datfin: '99999999',
    cie: { name: 'AXA' }
  },
  {
    _id: ObjectId('67ce048300bcc51dcd46ef65'),
    numper: '8079859',
    mut: '2',
    grp: '',
    entnum: '273993',
    entrai: 'AFCE FORMATION SAS',
    catcod: 'CAD',
    prd: 'AXA-AFCE CAD R.',
    opt: 'G R',
    fam: 'MAL',
    polnum: '2275781400000E19',
    poledi: '2275781400000E19',
    datdeb: '20220101',
    datfin: '99999999',
    cie: { name: 'AXA' }
  },
  {
    _id: ObjectId('67ce048300bcc51dcd46ef66'),
    numper: '8079874',
    mut: '2',
    grp: '',
    entnum: '273993',
    entrai: 'AFCE FORMATION SAS',
    catcod: 'CAD',
    prd: 'AXA-AFCE CAD R.',
    opt: 'L R',
    fam: 'MAL',
    polnum: '2275781400000E19',
    poledi: '2275781400000E19',
    datdeb: '20220101',
    datfin: '99999999',
    cie: { name: 'AXA' }
  },
  {
    _id: ObjectId('67ce048300bcc51dcd46ef67'),
    numper: '8079930',
    mut: '2',
    grp: '',
    entnum: '273993',
    entrai: 'AFCE FORMATION SAS',
    catcod: 'KCA',
    prd: 'AXA-AFCE CAD R.',
    opt: 'L R',
    fam: 'MAL',
    polnum: '2275781400000E19',
    poledi: '2275781400000E19',
    datdeb: '20220101',
    datfin: '99999999',
    cie: { name: 'AXA' }
  }

  Cas n°4 