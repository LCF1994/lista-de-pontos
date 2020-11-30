import os
from pybrsql import Pybrsql
import json


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
   

def query_pontos(bd, tipo):

    if check_iccp(bd):
        iccp = 'p{t}s.idiccp as id_iccp,'
    else:
        iccp = ''

    bd.consulta_bd('''
    select distinct
    idtdd
    from p{}d
    '''.format(tipo))
    
    colunas, joins = endereco_distribuicao(
        json.loads(brsql.tojson()), tipo)

    query = '''select
    p{t}s.id as id,
    p{t}s.nome as nome,
    p{t}s.idocr as ocr,
    p{t}s.idlia as rele,
    COALESCE(tcl.id, 'NLCL') as calculo,
    {}
    COALESCE(p{t}f.id, '') as end,
    {}
    FROM p{t}s
    left join p{t}f
    on p{t}s.a_p{t}f = p{t}f.br_rowid
    left join tcl
    on p{t}s.a_tcl = tcl.br_rowid
    {}
    where p{t}s.idlia !=''
    '''.format(iccp, colunas, joins, t=tipo)
    return query


def endereco_distribuicao(lista_tdds, tipo):
    coluna = ''
    join = ''
    for i, tdd in enumerate(lista_tdds):
        coluna += "COALESCE(p{t}f{}.id,'') as endereco_{}, ".format(
            i, tdd['idtdd'], t=tipo)

        join += '''
        left join (select * from p{t}d 
        where idtdd = '{}') as p{t}d{i}
        on p{t}s.id = p{t}d{i}.idp{t}s
        left join p{t}f as p{t}f{i}
        on p{t}d{i}.a_p{t}f = p{t}f{i}.br_rowid 
        '''.format(tdd['idtdd'], i=i, t=tipo)

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
        # query = query_digital(bd)
        query = query_pontos(bd, 'd')
        nome_do_arquivo = 'Template_de_pontos_digitais.csv'
        exibicao = 'Pontos Digitais'
    elif tipo == 'analogico':
        # query = query_analogica(bd)
        query = query_pontos(bd, 'a')
        nome_do_arquivo = 'Template_de_pontos_analogicos.csv'
        exibicao = 'Pontos Analogicos'
    elif tipo == 'comandos':
        query = query_comandos()
        nome_do_arquivo = 'Template_de_comandos.csv'
        exibicao = 'Comandos Configurados'
    else:
        # "Tipos de consultas : digital, analogico, comandos"  
        return  # erro de querisicao

    bd.consulta_bd(query)
    resultado = bd.tocsv()

    if arquivo:
        gera_arquivo(resultado, nome_do_arquivo)
        print('Arquivo gerado com o nome : {}'. format(nome_do_arquivo))

    if contagem:
        exibe_resultados(bd, exibicao, query)


def check_iccp(bd):
    query = "select count(*) as id from pro where id like '%%iccp%' "
    bd.consulta_bd(query)
    resultado = json.loads(bd.tojson())

    return bool(resultado[0]['id'])


if __name__ == '__main__':
    contexto = os.environ['BD']
    # contexto = '/export/home/sagetr1/sage/config/mt_eln/bd'
    # contexto = '/export/home/sagetr1/sage/config/aie_gvs/bd'
    brsql = Pybrsql(ct_or_path=contexto, source='xdr')

    consulta(brsql, 'digital', arquivo=True)
    consulta(brsql, 'analogico', arquivo=True)
    consulta(brsql, 'comandos', arquivo=True)
    
