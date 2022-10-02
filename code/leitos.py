import pandas as pd
import numpy as np
import requests
import pathlib
import sqlalchemy
import logging
import os
from dotenv import load_dotenv
from datetime import datetime
from bs4 import BeautifulSoup
from typing import List, Dict

project_dir = str(pathlib.Path(__file__).parent.parent.resolve())
file_dir    = str(pathlib.Path(__file__).parent.resolve())

load_dotenv(project_dir+'/config/config_file.env')
max_request_retries = int(os.getenv('MAX_REQUEST_RETRIES'))
request_timeout     = int(os.getenv('REQUEST_TIMEOUT'))

logging.basicConfig(filename=project_dir+'/log_file.log')

class MaxRequestRetries(Exception):
    pass
class InvalidFormatError(Exception):
    pass

def exportar_dataframe(engine: sqlalchemy.engine, df: pd.DataFrame, filedir: str, 
                       table_name: str, format: str ='excel', index: bool =False) -> None:
    """ Exporta o dataframe para o formato especificado

    Parametros
    ----------
    df: pandas.Dataframe
        Dataframe para ser exportado
    filedir: string
        Diretorio da pasta para output
    table_name: string
        Nome do arquivo output, sem a extensão
    format: string, default='excel'
        Formato do arquivo output, cujas opções estão no dicionário dict_exportar
    index: boolean, default=False
        Quando igual a True, a coluna de índice do Dataframe irá paro arquivo output
    engine: sqlalchemy.engine
        Conexão com o banco de dados, quando format='SQL'

    Retorno:
    ----------
        None
    """
    dict_exportar = {
        'EXCEL'  : [df.to_excel,'.xlsx'],
        'CSV'    : [df.to_csv,'.csv'],
        'PARQUET': [df.to_parquet,'.parquet'],
        'PICKLE' : [df.to_pickle,'.pickle'],
        'JSON'   : [df.to_json,'.json'],
        'SQL'    : [df.to_sql,'']
    }
    
    format = format.upper()
    try:
        export_func, extension = dict_exportar[format]
    except KeyError:
        logging.error(f"""{datetime.now()}: Formato inválido para exportação. Os formatos disponíveis são: {list(dict_exportar.keys())}.""")
        raise InvalidFormatError(f'Formato inválido para exportação. Os formatos disponíveis são: {list(dict_exportar.keys())}.')
    
    if format == 'SQL':
        export_func(name = table_name,con=engine,if_exists='append',index=index)
    else:
        export_func(filedir+'/'+table_name+extension,index=index)
    return None

def listar_links_tabelas_por_uf(uf: str) -> List[str]:
    """ Extrai os links das paginas que contém as tabelas de leitos, dado UF

    Parametros
    ----------
    uf: string
        Sigla do UF para coleta da tabela de leitos
    
    Retorno
    ----------
    links_tabelas: list[string]
        Lista com os links das paginas que contém as tabelas de leitos
    
    """
    codigo_ibge : dict = {'RJ':33, 'SP':35, 'ES':32, 'MG':31, 'SC':42,
                          'RS':43, 'PR':41, 'DF':53, 'GO':52, 'MT':51,
                          'MS':50, 'MA':21, 'PI':22, 'CE':23, 'RN':24,
                          'PB':25, 'PE':26, 'AL':27, 'SE':28, 'BA':29,
                          'RO':11, 'AC':12, 'AM':13, 'RR':14, 'PA':15,
                          'AP':16, 'TO':17}
    
    url = f'http://cnes2.datasus.gov.br/Mod_Ind_Tipo_Leito.asp?VEstado={codigo_ibge[uf]}'
    
    request_retries     = 0
    sucess              = False
    while not sucess:
        try:
            page_source       = requests.get(url,'lxml',timeout=request_timeout).text
            soup              = BeautifulSoup(page_source,'lxml')
            content           = soup.find('table',{'border':'1', 'align':'center'})
            descricoes_leitos = content.find_all('a')
            links_tabelas     = ['http://cnes2.datasus.gov.br/'+row.get('href') 
                                  for row in descricoes_leitos]
            
            quant_links = len(links_tabelas)
            if quant_links == 0: 
                sucess = False
                continue
            else: 
                sucess = True
                continue
        except requests.ReadTimeout:
            sucess = False
            continue
        except requests.ConnectTimeout:
            sucess = False
            continue
        except requests.exceptions.ConnectionError:
            logging.critical(
                f"""{datetime.now()}: Erro ao tentar estabelecer conexão com a API.
                Por favor, verifique sua conexão com a internet, ou tente novamente em outro momento.
                """)
            raise requests.ConnectionError()
        
        finally:
            request_retries += 1
            if request_retries >= max_request_retries:
                logging.error(
                f"""{datetime.now()}: Máximo de tentativas de request para o url: {url}. 
                Foram feitas {max_request_retries} tentativas com {request_timeout} segundos de timeout.
                """)
                raise MaxRequestRetries('API instável. Por favor, tente novamente em outro momento.')
    return links_tabelas

def ler_tabela_de_link(url: str, uf: str) -> List[Dict]:
    """ Lê a tabela de leitos contida no link dado

    Parametros
    ----------
    url: string
        Link da página contendo leitos de um tipo e especialidade, de dado UF
    uf: string
        Sigla do UF do qual está sendo coletada a tabela de leitos

    Retorno
    ----------
    table: List[Dict]
        Tabela de leitos de um tipo e especialidade
    """
    request_retries     = 0
    sucess              = False
    while not sucess:
        try:
            page_source = requests.get(url,'lxml',timeout=request_timeout).text
            soup = BeautifulSoup(page_source,'lxml')
            classificacao_leito = soup.find_all('font',{'color':'#ffcc99', 'face':'verdana,arial', 
                                                'size': '1'})[1].text.split(' - ')
            sucess = True
            continue
        except IndexError:
            sucess = False
            continue
        except requests.ReadTimeout:
            sucess = False
            continue
        except requests.ConnectTimeout:
            sucess = False
            continue
        except requests.ConnectionError:
            logging.critical(
                f""" {datetime.now()}: Erro ao tentar estabelecer conexão com a API.
                Por favor, verifique sua conexão com a internet, ou tente novamente em outro momento.
                """)
            raise requests.ConnectionError()
                
        finally:
            request_retries += 1
            if request_retries >= max_request_retries:
                logging.error(
                f""" {datetime.now()}: Máximo de tentativas de request para o url: {url}. 
                Foram feitas {max_request_retries} tentativas com {request_timeout} segundos de timeout.""")
                
                raise MaxRequestRetries('API instável. Por favor, tente novamente em outro momento.')
                
    tipo_leito          = classificacao_leito[1][1:].upper()
    especialidade_leito = classificacao_leito[-1].upper()

    content = soup.find('table',{'border':'1', 'align':'center'})
    rows  = content.find_all('tr',{'bgcolor':'#cccccc'})
    table  = []

    for row in rows:
        
        columns = [column.text for column in row.find_all('td')]
        cnes            = columns[0]
        estabelecimento = columns[1].replace("\n",'')
        municipio       = columns[2]
        existentes      = int(columns[3])
        sus             = int(columns[4])

        table.append({'CNES': cnes, 'ESTABELECIMENTO': estabelecimento, 'UF': uf, 
                       'MUNICIPIO': municipio, 'TIPO': tipo_leito, 'ESPECIALIDADE': especialidade_leito, 
                       'EXISTENTES': existentes, 'SUS': sus, 'NAO_SUS': existentes-sus})
    return table

def tab_leitos_por_uf(uf: str, exportar: bool =True, table_name: str ='', 
                      format: str ='excel', index: bool =False, 
                      engine: sqlalchemy.engine =None) -> pd.DataFrame:
    """Coleta a tabela de leitos completa, para dado UF

    Parametros
    -----------
    uf: string
        Sigla do UF para coleta da tabela de leitos
    exportar: boolean, default=True
        Quando igual a True, exporta a tabela para um arquivo de formato especificado
    table_name: string, default='Leitos_UF'
        Nome da tabela output, sem a extensão, quando exportar=True
    format: string, default='excel'
        Formato do arquivo, output, quando exportar=True. Os formatos disponíveis estão
        listados na função exportar_dataframe
    index: boolean, default=False
        Quando igual a True, inclui a coluna de índice do dataframe no arquivo output
    engine: sqlalchemy.engine, default=None
        Conexão com o banco de dados, quando exportar=True e format='SQL'
    
    Retorno
    -----------
    df_leitos_uf: pd.Dataframe
        Tabela de leitos completa para o UF dado
    """
    links_tables  = listar_links_tabelas_por_uf(uf)
    tab_leitos_uf = []

    quant_links = len(links_tables)
    for i,link in enumerate(links_tables):
        print(f'\rUF: {uf}. Lendo tabela {i+1} de {quant_links}.',end='')
        tab = ler_tabela_de_link(link,uf)
        tab_leitos_uf = tab_leitos_uf + tab
    df_leitos_uf = pd.DataFrame.from_records(tab_leitos_uf).astype({
        'CNES':np.str_, 'ESTABELECIMENTO':np.str_, 'UF':'category', 'TIPO':'category',
        'ESPECIALIDADE':'category', 'EXISTENTES':np.int32, 'SUS':np.int32, 'NAO_SUS':np.int32
    })
    
    if exportar:
        project_output_dir = project_dir+'/output'
        if table_name=='': 
            table_name = f'Leitos_{uf}'
        exportar_dataframe(df=df_leitos_uf,format=format,index=index,filedir=project_output_dir,
                           table_name=table_name, engine=engine)
    return df_leitos_uf

def tab_leitos_brasil(exportar: bool =True, table_name: str ='Leitos_Brasil', 
                      format: str ='excel', index: bool =False, 
                      engine: sqlalchemy.engine =None) -> pd.DataFrame:
    """Coleta a tabela de leitos completa do Brasil

    Parametros
    -----------
    exportar: boolean, default=True
        Quando igual a True, exporta a tabela para um arquivo de formato especificado
    table_name: string, default='Leitos_Brasil'
        Nome da tabela output, sem a extensão, quando exportar=True
    format: string, default='excel'
        Formato do arquivo, output, quando exportar=True. Os formatos disponíveis estão
        listados na função exportar_dataframe
    index: boolean, default=False
        Quando igual a True, inclui a coluna de índice do dataframe no arquivo output
    engine: sqlalchemy.engine, default=None
        Conexão com o banco de dados, quando exportar=True e format='SQL'
    
    Retorno
    -----------
    df_leitos_Brasil: pd.Dataframe
        Tabela de leitos completa do Brasil
    """
    UFs = ['RJ', 'SP', 'ES', 'MG', 'SC', 'RS', 'PR', 'DF', 'GO', 
           'MT', 'MS', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 
           'SE', 'BA', 'RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO']

    df_leitos_brasil = pd.DataFrame(columns=['CNES', 'ESTABELECIMENTO', 'UF', 'MUNICIPIO', 'TIPO', 
                                             'ESPECIALIDADE', 'EXISTENTES', 'SUS', 'NAO_SUS'])
    df_leitos_brasil = df_leitos_brasil.astype({
        'CNES':np.str_, 'ESTABELECIMENTO':np.str_, 'UF':'category', 'TIPO':'category',
        'ESPECIALIDADE':'category', 'EXISTENTES':np.int32, 'SUS':np.int32, 'NAO_SUS':np.int32
    })

    print(f'UFs para coleta: {UFs}')
    for uf in UFs:
        df_leitos_uf     = tab_leitos_por_uf(uf=uf,exportar=False)
        df_leitos_brasil = pd.concat([df_leitos_brasil,df_leitos_uf])
    print('\n')
    
    if exportar: 
        project_output_dir = project_dir+'/output'
        exportar_dataframe(df=df_leitos_brasil, format=format, index=index,
                           filedir=project_output_dir, table_name=table_name, engine=engine)
    return df_leitos_brasil