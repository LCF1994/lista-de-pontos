# Lista de Pontos

Script baseado no modulo Pybrsql e desenvolvido para ser utilizado no ambiente do SAGE.


## Objetivo

O objetivo desse Script é gerar uma lista de pontos de maneira rápida e fácil.


## Modo de uso

1. Mova o script "lista_de_pontos.py" para a pasta $SAGE na máquina onde deseja utilizar.
2. Execute o comando : python lista_de_pontos.py


## Resultado esperado

O script irá mostar na tela:
    - Número de pontos digitais;
    - Nome do arquivo gerado contendo os pontos digitais;
    - Número de pontos analógicos;
    - Nome do arquivo gerado contendo os pontos analógicos;
    - Número de Comandos configurados;
    - Nome do arquivo com os comandos configurados


## Funcionamento

Através do módulo Pybrsql, o script faz uma série de consultas nos arquivos xdr e 
a partir do resultado dessas consultas faz uma contagem de resultados e exporta 3 arquivos csv.

O caminho para os arquivos é definido por um contexto, por padrão $BD está sendo utilizado, porém
outros caminhos para outras bases existentes na máquina podem ser utilizados.

As consultas são feitas individualmente para pontos digitais, analógicos ou comandos configurados.
Cada consulta executa uma função que tem como parametros:
    - brsql: objeto Pybrsql inicializado conforme contexto da base de dados a ser consultada;
    - tipo: Consulta ('digital', 'analogico', 'comandos');
    - arquivo: Flag binária indicando se haverá ou não exportação de arquivo da consulta;
    - contagem: Flag binária indicando se será realizada e exibida a contagem de resultados


## Testes

Para realização de testes é necessario ter um ambiente SAGE. 
O principal módulo utilizado é de propriedade da CEPEL e apenas dentro do ambiente SAGE.


## Informações Importantes

SAGE é uma sigla do Sistema Aberto de Gerenciamento de Energia desenvolvido pela CEPEL.
Versão utlizada para testes e desenvolvimento é 28.0
O módulo Pybrsql foi desenvolvido pela CEPEL.
