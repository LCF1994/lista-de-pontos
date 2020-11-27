import os
from pybrsql import Pybrsql
import json


#path = os.environ['BD']
path = '/export/home/sagetr1/sage/config/mt_eln/bd'
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
brsql.consulta_bd(QUERY_COMANDOS)
comandos = brsql.tocsv()


def cria_query_digital(bd):

    bd.consulta_bd('''
    select distinct
    idtdd
    from pdd
    ''')
    
    colunas, joins = anexa_endereco_distribuicao_digital(
        json.loads(brsql.tojson()))

    query = '''select
    pds.id as id,
    pds.nome as nome,
    pds.idocr as ocr,
    pds.idlia as rele,
    COALESCE(tcl.id, 'NLCL') as calculo,
    COALESCE(pdf.id, '') as end,
    {}
    FROM pds
    left join pdf
    on pds.a_pdf = pdf.br_rowid
    left join tcl
    on pds.a_tcl = tcl.br_rowid
    {}
    where pds.idlia !=''
    '''.format(colunas, joins)
    return query


def anexa_endereco_distribuicao_digital(lista_tdds):
    coluna = ''
    join = ''
    for i, tdd in enumerate(lista_tdds):
        coluna += 'pdf{}.id as endereco_{}, '.format(i, tdd['idtdd'])

        join += '''
        left join (select * from pdd where idtdd = '{}') as pdd{}
        on pds.id = pdd{}.idpds
        left join pdf as pdf{}
        on pdd{}.a_pdf = pdf{}.br_rowid 
        '''.format(tdd['idtdd'], i, i, i, i, i)

    coluna = coluna[:-2]

    return coluna, join


def cria_query_analogica(bd):

    bd.consulta_bd('''
    select distinct
    idtdd
    from pad
    ''')
    
    colunas, joins = anexa_endereco_distribuicao_analogico(
        json.loads(brsql.tojson()))

    query = '''select
    pas.id as id,
    pas.nome as nome,
    pas.idocr as ocr,
    pas.idlia as rele,
    COALESCE(tcl.id, 'NLCL') as calculo,
    COALESCE(paf.id, '') as end,
    {}
    FROM pas
    left join paf
    on pas.a_paf = paf.br_rowid
    left join tcl
    on pas.a_tcl = tcl.br_rowid
    {}
    where pas.idlia !=''
    limit 20
    '''.format(colunas, joins)
    return query


def anexa_endereco_distribuicao_analogico(lista_tdds):
    coluna = ''
    join = ''
    for i, tdd in enumerate(lista_tdds):
        coluna += 'paf{}.id as endereco_{}, '.format(i, tdd['idtdd'])

        join += '''
        left join (select * from pad where idtdd = '{}') as pad{}
        on pas.id = pad{}.idpas
        left join paf as paf{}
        on pad{}.a_paf = paf{}.br_rowid 
        '''.format(tdd['idtdd'], i, i, i, i, i)

    coluna = coluna[:-2]

    return coluna, join


brsql.consulta_bd(cria_query_digital(brsql))
brsql.tabula()

brsql.consulta_bd('select count(*) as pontos from ({})'.format(
    cria_query_digital(brsql)))
brsql.tabula()
print(brsql.tocsv())
print(brsql.tojson())

brsql.consulta_bd(cria_query_analogica(brsql))
brsql.tabula()

query = '''select 
pdd.*, pdf.id as endereco
from pdd
join pdf
on pdd.a_pdf = pdf.br_rowid
where idpds = 'MTSY601POS'

'''
brsql.consulta_bd(query)
brsql.tabula()

query2 = '''select 
id, nome, p_pdd, br_rowid
from pds 
where id = 'MTSY601POS'
'''

brsql.consulta_bd(query2)
brsql.tabula()

query3 = '''select distinct
idtdd
from pdd
'''

nome_do_arquivo = 'lista_de_pontos_siemens.csv'
file_path = os.environ['SAGE'] + '/' + nome_do_arquivo

with open(file_path, 'write') as f:
    f.writelines(pontos_digitais)
