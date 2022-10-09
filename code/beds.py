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

def export_dataframe(engine: sqlalchemy.engine, df: pd.DataFrame, filedir: str, 
                       table_name: str, format: str ='excel', index: bool =False) -> None:
    """ Export dataframe to specified format

    Parameters
    ----------
    df: pandas.Dataframe
        Dataframe to export
    filedir: string
        Directory of output folder
    table_name: string
        Output filename, without extension
    format: string, default='excel'
        Output file format, whose options are in the dictionary dict_export
    index: boolean, default=False
        When True, the index column will appear in the output file
    engine: sqlalchemy.engine
        Database connection, when format='SQL'

    Return:
    ----------
        None
    """
    dict_export = {
        'EXCEL'  : [df.to_excel,'.xlsx'],
        'CSV'    : [df.to_csv,'.csv'],
        'PARQUET': [df.to_parquet,'.parquet'],
        'PICKLE' : [df.to_pickle,'.pickle'],
        'JSON'   : [df.to_json,'.json'],
        'SQL'    : [df.to_sql,'']
    }
    
    format = format.upper()
    try:
        export_func, extension = dict_export[format]
    except KeyError:
        logging.error(f"""{datetime.now()}: Invalid format to export. The available formats are: {list(dict_export.keys())}.""")
        raise InvalidFormatError(f'Invalid format to export. The available formats are: {list(dict_export.keys())}.')
    
    if format == 'SQL':
        export_func(name = table_name,con=engine,if_exists='append',index=index)
    else:
        export_func(filedir+'/'+table_name+extension,index=index)
    return None

def list_table_links_by_uf(uf: str) -> List[str]:
    """ Extract the links of pages containing bed tables, given UF

    Parameters
    ----------
    uf: string
        UF acronym for collection of the bed table
    
    Return
    ----------
    links_tabelas: list[string]
        List with links of pages containing bed tables
    
    """
    cod_ibge : dict = {'RJ':33, 'SP':35, 'ES':32, 'MG':31, 'SC':42,
                          'RS':43, 'PR':41, 'DF':53, 'GO':52, 'MT':51,
                          'MS':50, 'MA':21, 'PI':22, 'CE':23, 'RN':24,
                          'PB':25, 'PE':26, 'AL':27, 'SE':28, 'BA':29,
                          'RO':11, 'AC':12, 'AM':13, 'RR':14, 'PA':15,
                          'AP':16, 'TO':17}
    
    url = f'http://cnes2.datasus.gov.br/Mod_Ind_Tipo_Leito.asp?VEstado={cod_ibge[uf]}'
    
    request_retries     = 0
    sucess              = False
    while not sucess:
        try:
            page_source      = requests.get(url,'lxml',timeout=request_timeout).text
            soup             = BeautifulSoup(page_source,'lxml')
            content          = soup.find('table',{'border':'1', 'align':'center'})
            bed_descriptions = content.find_all('a')
            links_tables     = ['http://cnes2.datasus.gov.br/'+row.get('href') 
                                 for row in bed_descriptions]
            
            quant_links = len(links_tables)
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
                f"""{datetime.now()}: Error while trying to establish connection with APi.
                Please verify your internet connection, or try again at another time.
                """)
            raise requests.ConnectionError()
        
        finally:
            request_retries += 1
            if request_retries >= max_request_retries:
                logging.error(
                f"""{datetime.now()}: Maximum request attempts to url: {url}.
                Were made {max_request_retries} attempts with {request_timeout} seconds timeout.
                """)
                raise MaxRequestRetries('API current unstable. Please try again at another time.')
    return links_tables

def read_table_from_link(url: str, uf: str) -> List[Dict]:
    """ Read bed table contained in the given link

    Parameters
    ----------
    url: string
        Link of page containing beds with a type and speciality, given UF acronym
    uf: string
        Acronym of the UF from which the bed table is being collected

    Return
    ----------
    table: List[Dict]
        Bed table with a type and speciality
    """
    request_retries     = 0
    sucess              = False
    while not sucess:
        try:
            page_source = requests.get(url,'lxml',timeout=request_timeout).text
            soup = BeautifulSoup(page_source,'lxml')
            bed_classification = soup.find_all('font',{'color':'#ffcc99', 'face':'verdana,arial', 
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
                f""" {datetime.now()}: Error while trying to establish connection with APi.
                Please verify your internet connection, or try again at another time.
                """)
            raise requests.ConnectionError()
                
        finally:
            request_retries += 1
            if request_retries >= max_request_retries:
                logging.error(
                f""" {datetime.now()}: Maximum request attempts to url: {url}. 
                Were made {max_request_retries} attempts with {request_timeout} seconds timeout.""")
                
                raise MaxRequestRetries('API current unstable. Please try again at another time.')
                
    bed_type       = bed_classification[1][1:].upper()
    bed_speciality = bed_classification[-1].upper()

    content = soup.find('table',{'border':'1', 'align':'center'})
    rows  = content.find_all('tr',{'bgcolor':'#cccccc'})
    table  = []

    for row in rows:
        
        columns = [column.text for column in row.find_all('td')]
        cnes          = columns[0]
        establishment = columns[1].replace("\n",'')
        city          = columns[2]
        existent      = int(columns[3])
        sus           = int(columns[4])

        table.append({'CNES': cnes, 'ESTABELECIMENTO': establishment, 'UF': uf, 
                      'MUNICIPIO': city, 'TIPO': bed_type, 'ESPECIALIDADE': bed_speciality, 
                      'EXISTENTES': existent, 'SUS': sus, 'NAO_SUS': existent-sus})
    return table

def bed_tab_by_uf(uf: str, export: bool =True, table_name: str ='', format: str ='excel', 
                    index: bool =False, engine: sqlalchemy.engine =None) -> pd.DataFrame:
    """Coleta a tabela de leitos completa, para dado UF

    Parameters
    -----------
    uf: string
        UF acronym for collection of the bed table
    export: boolean, default=True
        When True, export table to specified format
    table_name: string, default='Leitos_UF'
        Output filename, without extension, when export=True
    format: string, default='excel'
        Output file format, when export=True. Available formats are in 
        the function export_dataframe
    index: boolean, default=False
        When True, the index column will appear in the output file
    engine: sqlalchemy.engine, default=None
        Database connection, when export=True e format='SQL
    
    Return
    -----------
    df_leitos_uf: pd.Dataframe
        Complete bed table for given UF
    """
    links_tables  = list_table_links_by_uf(uf)
    uf_bed_tab = []

    quant_links = len(links_tables)
    for i,link in enumerate(links_tables):
        print(f'\rUF: {uf}. Reading table {i+1} out of {quant_links}.',end='')
        tab = read_table_from_link(link,uf)
        uf_bed_tab = uf_bed_tab + tab

    df_uf_beds = pd.DataFrame.from_records(uf_bed_tab).astype({
        'CNES':np.str_, 'ESTABELECIMENTO':np.str_, 'UF':'category', 'TIPO':'category',
        'ESPECIALIDADE':'category', 'EXISTENTES':np.int32, 'SUS':np.int32, 'NAO_SUS':np.int32
    })

    if export:
        project_output_dir = project_dir+'/output'
        if table_name=='': 
            table_name = f'{uf}_Beds'
        export_dataframe(df=df_uf_beds,format=format,index=index,filedir=project_output_dir,
                           table_name=table_name, engine=engine)
    return df_uf_beds

def brazil_bed_tab(export: bool =True, table_name: str ='Brazil_Beds', 
                      format: str ='excel', index: bool =False, 
                      engine: sqlalchemy.engine =None) -> pd.DataFrame:
    """Collect complete Brazil's bed table

    Parameters
    -----------
    export: boolean, default=True
        When True, export table to specified format
    table_name: string, default='Leitos_Brasil'
        Output filename, without extension, when export=True
    format: string, default='excel'
        Output file format, when export=True. Available formats are in 
        the function export_dataframe
    index: boolean, default=False
        When True, the index column will appear in the output file
    engine: sqlalchemy.engine, default=None
        Database connection, when export=True e format='SQL
    
    Return
    -----------
    df_brazil_beds: pd.Dataframe
        Complete Brazil's bed table
    """
    UFs = ['RJ', 'SP', 'ES', 'MG', 'SC', 'RS', 'PR', 'DF', 'GO', 
           'MT', 'MS', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 
           'SE', 'BA', 'RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO']

    df_brazil_beds = pd.DataFrame(columns=['CNES', 'ESTABELECIMENTO', 'UF', 'MUNICIPIO', 'TIPO', 
                                           'ESPECIALIDADE', 'EXISTENTES', 'SUS', 'NAO_SUS'])
    df_brazil_beds = df_brazil_beds.astype({
        'CNES':np.str_, 'ESTABELECIMENTO':np.str_, 'UF':'category', 'TIPO':'category',
        'ESPECIALIDADE':'category', 'EXISTENTES':np.int32, 'SUS':np.int32, 'NAO_SUS':np.int32
    })

    print(f'UFs to collect: {UFs}')
    for uf in UFs:
        df_uf_beds     = bed_tab_by_uf(uf=uf,export=False)
        df_brazil_beds = pd.concat([df_brazil_beds,df_uf_beds])
    print('\n')
    
    if export: 
        project_output_dir = project_dir+'/output'
        export_dataframe(df=df_brazil_beds, format=format, index=index,
                           filedir=project_output_dir, table_name=table_name, engine=engine)
    return df_brazil_beds