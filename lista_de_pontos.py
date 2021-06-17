# Programa Desenvolvido por Lucas Campos Ferreira

import os
from pybrsql import Pybrsql
import json


def query_comandos():
    return (
        'SELECT '
        'cgs.id, '
        'cgs.nome, '
        'cgs.idpac AS feedback, '
        'tctl.dlg_close AS comando_1, '
        'tctl.dlg_trip AS comando_0, '
        'cgs.idpit AS intertravamento, '
        'cgf.id AS end_aquisicao, '
        'cgf.kconv, '
        "COALESCE(cgf2.id,'') AS end_dist "
        'FROM cgs '
        'LEFT JOIN cgf ON cgs.a_cgf = cgf.br_rowid '
        'LEFT JOIN cgf as cgf2 '
        'ON cgs.id = REPLACE(cgf2.kconv, "CGS= ", "") '
        'LEFT JOIN tctl '
        'ON cgs.a_tctl = tctl.br_rowid'
    )


def query_pontos(bd, tipo):

    if check_iccp(bd):
        iccp = 'p{t}s.idiccp AS id_iccp,'.format(t=tipo)
    else:
        iccp = ''

    bd.consulta_bd(
        (
            "SELECT DISTINCT idtdd FROM p{t}d"
        ).format(t=tipo))

    colunas, joins = endereco_distribuicao(
        json.loads(brsql.tojson()), tipo)

    if tipo == 'd':
        colunas = ', ocr1.texto AS valor_1, ocr2.texto AS valor_0' + colunas
        joins = ' LEFT JOIN ocr AS ocr2 ON pds.a_ocr=ocr2.br_rowid-4 ' + joins
        joins = ' LEFT JOIN ocr AS ocr1 ON pds.a_ocr=ocr1.br_rowid-3 ' + joins

    query = (
        'SELECT '
        'p{t}s.id AS id, '
        'p{t}s.nome AS nome, '
        'p{t}s.idocr AS ocr, '
        'p{t}s.idlia AS rele, '
        "COALESCE(tcl.id, 'NLCL') AS calculo, "
        '{}'
        "COALESCE(p{t}f.id, '') AS end {} "
        'FROM p{t}s '
        'LEFT JOIN p{t}f '
        'ON p{t}s.a_p{t}f = p{t}f.br_rowid '
        'LEFT JOIN tcl '
        'ON p{t}s.a_tcl = tcl.br_rowid '
        '{} '
        "WHERE p{t}s.idlia !='' "
        ).format(iccp, colunas, joins, t=tipo)

    return query


def endereco_distribuicao(lista_tdds, tipo):
    coluna, join = '', ''

    for i, tdd in enumerate(lista_tdds):
        coluna += ", COALESCE(p{t}f{}.id,'') AS 'endereco_{}' ".format(
            i, tdd['idtdd'], t=tipo)

        join += (
            "LEFT JOIN ( "
            "SELECT id, idp{t}s, a_p{t}f FROM p{t}d "
            "where idtdd LIKE '{}' "
            ") AS p{t}d{i} "
            "ON p{t}s.id=p{t}d{i}.idp{t}s "
            "LEFT JOIN p{t}f AS p{t}f{i} "
            "ON p{t}d{i}.a_p{t}f=p{t}f{i}.br_rowid "
        ).format(tdd['idtdd'], i=i, t=tipo)

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
        query = query_pontos(bd, 'd')
        nome_do_arquivo = 'Template_de_pontos_digitais.csv'
        exibicao = 'Pontos Digitais'
    elif tipo == 'analogico':
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
    # contexto = '/export/home/sagetr1/sage/config/{}/bd'.format(nome_da_base)
    brsql = Pybrsql(ct_or_path=contexto, source='xdr')

    consulta(brsql, 'digital', arquivo=True, contagem=True)
    consulta(brsql, 'analogico', arquivo=True, contagem=True)
    consulta(brsql, 'comandos', arquivo=True, contagem=True)
