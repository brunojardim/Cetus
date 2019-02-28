############################### PRODUCAO ####################################
############################### DataLake ####################################
# Autor: Bruno Jaja                                                         #
# Data: 19/02/2019                                                          #
# Assunto: Receita Publica                                                  #
# Defasagem D - 1                                                           #  
#############################################################################
#############################################################################


#############################################################################
# ------------------------ INICIO FUNCOES -----------------------------------

def CetusLakeReceitaPublicaGov(ANO_INFO,exec_lake = 'prod'):
    
    #---- Pagina de Extracao -----------
    # http://www.portaltransparencia.gov.br/download-de-dados/receitas
    
    #---- Metadados do Arquivo ---------
    # N/A
    
    
    # ----- Pegando a Data da Informacao e Gravando na Tabela -------------------
    import requests
    from bs4 import BeautifulSoup
    import re
    
    if exec_lake == 'prod':
        print('CETUS --> Executando em Producao')
        # Utilizado para producao (execucao semanal ou mensal)
        URL = 'http://www.portaltransparencia.gov.br/origem-dos-dados'
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, 'html.parser')
		aux = soup.find_all('button', id="botao-aba-receitas")
		ddmmaaaa = re.sub("[^\d\.]", "", str(aux))[-8:]
		#print('Data em formato ddmmaaaa:', ddmmaaaa)
		ano = re.sub("[^\d\.]", "", str(ddmmaaaa))[-4:]
		mes = re.sub("[^\d\.]", "", str(ddmmaaaa))[2:-4]
		dia = re.sub("[^\d\.]", "", str(ddmmaaaa))[:-6]
		#print('Ano->',ano)
		#print('Mês->',mes)
		#print('Dia->',dia)
		datinfo = ano+mes+dia
		#print('A data da informação é: ', datinfo)
        # ----------------------------------------------------------------------------
    else:
        print('CETUS --> Executando Carga Historica')
        # Formato que pegamos no site quando no modo producao (AAAAMMDD)
        datinfo = str(ANO_INFO) + '12' + '31' 
        
    
   
    import urllib.request
    urllib.request.urlretrieve('http://www.portaltransparencia.gov.br/download-de-dados/receitas'+str(ANO_INFO), 'dados_receita_' + str(ANO_INFO) + '.zip')

    import zipfile
    with zipfile.ZipFile('dados_receita_' + str(ANO_INFO) + '.zip',"r") as zip_ref:
        zip_ref.extractall('dados_receita_' + str(ANO_INFO))

    import pandas as pd
    import chardet
    with open('dados_receita_' + str(ANO_INFO) +'/'+str(ANO_INFO)+'_ReceitaPublica.zip.csv', 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df = pd.read_csv('dados_receita_' + str(ANO_INFO) +'/'+str(ANO_INFO)+'_ReceitaPublica.zip.csv', encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df['DT_INFO'] = datinfo
    return df


def CetusSalvaCSVS3(dataframe,nm_s3_file,s3_path):

    import s3fs
    S3fs = s3fs.S3FileSystem()

    # Nome do arquivo a ser salvo no S3
    #nm_s3_file = 'OP_Lake_RecPub_'+str(datinfo)+'.csv'

    # Caminho no lake (S3)
    #s3_path = 'projeto-bigdata-cetus/datalake/ReceitasPublicas/'

    bytes_to_write = dataframe.to_csv(sep = ',',header=True, index=False, index_label=None).encode()
    with S3fs.open(s3_path+nm_s3_file, 'wb') as f:
        f.write(bytes_to_write)
    return print('Salvo com Sucesso :)')



def CetusReadS3CSVFile(nm_bucket,path_file,sep=','):
    import boto3
    bucket=nm_bucket # Or whatever you called your bucket
    data_key = path_file # Where the file is within your bucket
    data_location = 's3://{}/{}'.format(bucket, data_key)
    try:
        df = pd.read_csv(data_location,sep=sep,encoding='utf-8',error_bad_lines=False) 

    except:
        df = pd.read_csv(data_location,sep=sep,encoding='latin-1',error_bad_lines=False)

    return df

################################################################################################
#------------------------ FIM FUNCOES ----------------------------------------------------------    





#import datetime
#now = datetime.datetime.now() - datetime.timedelta(0,0,0,0,3)
#anomesdia = now.strftime("%Y%m%d")
#anoref = now.strftime("%Y")

print('RP_Dev_Jupyter_RecPub')
print('Prod - Carga de referencia: ',datinfo)
dados = CetusLakeReceitaPublicaGov(anoref,exec_lake = 'prod')
nm_s3_file = 'RP_Lake_RecPub_'+str(datinfo)+'.csv'
s3_path = 'projeto-bigdata-cetus/datalake/ReceitasPublicas/'
CetusSalvaCSVS3(dados,nm_s3_file,s3_path)
print('Carga Efetuada Com Sucesso')