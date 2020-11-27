import os
from pybrsql import Pybrsql
import json


# contexto = os.environ['BD']
contexto = '/export/home/sagetr1/sage/config/mt_eln/bd'
brsql = Pybrsql(ct_or_path=contexto, source='xdr')


def query_comandos():
    return '''select
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
    

def query_digital(bd):

    bd.consulta_bd('''
    select distinct
    idtdd
    from pdd
    ''')
    
    colunas, joins = endereco_distribuicao_digital(
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


def endereco_distribuicao_digital(lista_tdds):
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


def query_analogica(bd):

    bd.consulta_bd('''
    select distinct
    idtdd
    from pad
    ''')
    
    colunas, joins = endereco_distribuicao_analogico(
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


def endereco_distribuicao_analogico(lista_tdds):
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


def exibe_resultados(bd, query):
    bd.consulta_bd('select count(*) as pontos from ({})'.format(query))
    bd.tabula()

    print(bd.tojson())


def gera_arquivo(conteudo, nome):
    file_path = os.environ['SAGE'] + '/' + nome

    with open(file_path, 'write') as f:
        f.writelines(conteudo)


brsql.consulta_bd(query_digital(brsql))
pontos_digitais = brsql.tocsv()
print('Pontos Digitais :')
brsql.tabula()
# gera_arquivo(pontos_digitais, 'Template_de_pontos_digitais.csv')

brsql.consulta_bd(query_analogica(brsql))
pontos_analogicos = brsql.tocsv()
print('Pontos Analogicos :')
brsql.tabula()
# gera_arquivo(pontos_digitais, 'Template_de_pontos_analogicos.csv')

brsql.consulta_bd(query_comandos())
print('Comandos Configurados :')
comandos = brsql.tabula()
# gera_arquivo(pontos_digitais, 'Template_de_comandos.csv')
