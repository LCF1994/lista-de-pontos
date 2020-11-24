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

query = '''select
id, nome, a_tctl, idpit, idpac
from cgs
'''
brsql.consulta_bd(query)
brsql.tabula()

file_path = os.environ['SAGE'] + '/lista_de_pontos_siemens.csv'

#with open(file_path, 'write') as f:
#    f.writelines(pontos_digitais)
