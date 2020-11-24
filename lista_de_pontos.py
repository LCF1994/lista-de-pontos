import os
from pybrsql import Pybrsql


path = os.environ['BD']
brsql = Pybrsql(ct_or_path=path, source='xdr')

query = '''select 
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

brsql.consulta_bd(query)

output = brsql.tocsv()

file_path = os.environ['SAGE'] + '/lista_de_pontos_siemens.csv'

with open(file_path, 'write') as f:
    f.writelines(output)
