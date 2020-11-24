import os
from pybrsql import Pybrsql


path = os.environ['BD']
brsql = Pybrsql(ct_or_path=path, source='xdr')

QUERY_PONTOS_DIGITAIS = '''select
pds.id as id,
pds.nome as nome,
pds.idocr as ocr,
pds.idlia as rele,
COALESCE(tcl.id, 'NLCL') as calculo,
COALESCE(pdf.id, '') as end
FROM pds
left join pdf
on pds.a_pdf = pdf.br_rowid
left join tcl
on pds.a_tcl = tcl.br_rowid
where pds.idlia !=''
'''
brsql.consulta_bd(QUERY_PONTOS_DIGITAIS)
pontos_digitais = brsql.tocsv()

QUERY_PONTOS_ANALOGICOS = '''select
pas.id as id,
pas.nome as nome,
pas.idocr as ocr,
pas.idlia as rele,
COALESCE(tcl.id, 'NLCL') as calculo,
COALESCE(paf.id, '') as end
FROM pas
left join paf
on pas.a_paf = paf.br_rowid
left join tcl
on pas.a_tcl = tcl.br_rowid
where pas.idlia !=''
'''
brsql.consulta_bd(QUERY_PONTOS_ANALOGICOS)
pontos_analogicos = brsql.tocsv()

QUERY_COMANDOS = '''select
cgs.id,
cgs.nome,
cgs.idpac as feedback,
tctl.dlg_close as comando_1,
tctl.dlg_trip as comando_0,
cgs.idpit as intertravamento,
cgf.id as end_aquisicao,
cgf.kconv,
COALESCE(cgf2.id,'') as end_dist
from cgs
left join cgf
on cgs.a_cgf = cgf.br_rowid
left join cgf as cgf2
on cgs.id = REPLACE(cgf2.kconv, "CGS= ", "")
left join tctl
on cgs.a_tctl = tctl.br_rowid
'''
brsql.consulta_bd(QUERY_PONTOS_DIGITAIS)
comandos = brsql.tocsv()

query_pontos_digitais_v2 = '''select
pds.id as id,
pds.nome as nome,
pds.idocr as ocr,
pds.idlia as rele,
COALESCE(tcl.id, 'NLCL') as calculo,
COALESCE(pdf.id, '') as end,
pdf2.id as end_dist
FROM pds
left join pdf
on pds.a_pdf = pdf.br_rowid
left join tcl
on pds.a_tcl = tcl.br_rowid
left join pdd
on pds.p_pdd = pdd.br_rowid
left join pdf as pdf2
on pdd.a_pdf = pdf2.br_rowid
where pds.idlia !=''
'''

query_pontos_analogicos_v2 = '''select
pas.id as id,
pas.nome as nome,
pas.idocr as ocr,
pas.idlia as rele,
COALESCE(tcl.id, 'NLCL') as calculo,
COALESCE(paf.id, '') as end,
paf2.id as end_dist
FROM pas
left join paf
on pas.a_paf = paf.br_rowid
left join tcl
on pas.a_tcl = tcl.br_rowid
left join pad
on pas.p_pad = pad.br_rowid
left join pdf as paf2
on pad.a_paf = paf2.br_rowid
where pas.idlia !=''
'''

brsql.consulta_bd(query_pontos_analogicos_v2)
brsql.tabula()

nome_do_arquivo = 'lista_de_pontos_siemens.csv'
file_path = os.environ['SAGE'] + '/' + nome_do_arquivo

with open(file_path, 'write') as f:
    f.writelines(pontos_digitais)
