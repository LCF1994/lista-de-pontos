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
        coluna += "COALESCE(pdf{}.id,'') as endereco_{}, ".format(
            i, tdd['idtdd'])

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
    '''.format(colunas, joins)
    return query


def endereco_distribuicao_analogico(lista_tdds):
    coluna = ''
    join = ''
    for i, tdd in enumerate(lista_tdds):
        coluna += "COALESCE(paf{}.id,'') as endereco_{}, ".format(
            i, tdd['idtdd'])

        join += '''
        left join (select * from pad where idtdd = '{}') as pad{}
        on pas.id = pad{}.idpas
        left join paf as paf{}
        on pad{}.a_paf = paf{}.br_rowid 
        '''.format(tdd['idtdd'], i, i, i, i, i)

    coluna = coluna[:-2]

    return coluna, join


def exibe_resultados(bd, tipo, query):
    bd.consulta_bd('select count(*) as pontos from ({})'.format(query))
    resultado = json.loads(bd.tojson())

    print('{} : {}'.format(tipo, resultado[0]['pontos']))


def gera_arquivo(conteudo, nome):
    file_path = os.environ['SAGE'] + '/' + nome

    with open(file_path, 'write') as f:
        f.writelines(conteudo)


def consulta(bd, tipo, arquivo=True, contagem=True):
    if not arquivo and not contagem:
        return

    if tipo == 'digital':
        query = query_digital(bd)
        nome_do_arquivo = 'Template_de_pontos_digitais.csv'
        exibicao = 'Pontos Digitais'
    elif tipo == 'analogico':
        query = query_analogica(bd)
        nome_do_arquivo = 'Template_de_pontos_analogicos.csv'
        exibicao = 'Pontos Analogicos'
    elif tipo == 'comandos':
        query = query_comandos()
        nome_do_arquivo = 'Template_de_comandos.csv'
        exibicao = 'Comandos Configurados'
    else:
        return  # erro de querisicao

    bd.consulta_bd(query)
    resultado = bd.tocsv()

    if arquivo:
        gera_arquivo(resultado, nome_do_arquivo)
        print('Arquivo gerado com o nome : {}'. nome_do_arquvio)

    if contagem:
        exibe_resultados(bd, exibicao, query)


if __name__ == '__main__':
    consulta(brsql, 'digital', arquivo=True)
    consulta(brsql, 'analogico', arquivo=True)
    consulta(brsql, 'comandos', arquivo=True)
