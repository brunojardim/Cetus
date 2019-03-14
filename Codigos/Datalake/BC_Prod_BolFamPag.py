# coding=utf-8
############################### PRODUCAO ####################################
############################### DataLake ####################################
# Autor: Bruno Jardim                                                       #
# Data: 13/03/2019                                                          #
# Assunto: Beneficios aos Cidadaos - Bolsa Familia - Pagamentos             #
# Defasagem: M - 4                                                          #  
#############################################################################
#############################################################################

print('##########################################')
print('------------- Projeto Cetus --------------')
print('--------------- Crawlers -----------------')
print('------- Beneficios aos Cidadaos ----------')
print('------ Bolsa Familia - Pagamentos --------')
print('##########################################')
#############################################################################
# ------------------------ INICIO FUNCOES -----------------------------------

def CetusLakeBeneficiosCidadaoBolsaFamilia(dataproc,ANOMES_INFO,exec_lake = 'prod'):
    
    #---- Pagina de Extracao -----------
    # http://www.portaltransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos
    
    #---- Metadados do Arquivo ---------
    # http://www.portaltransparencia.gov.br/pagina-interna/603397-dicionario-de-dados-bolsa-familia-pagamentos
    
    
    # ----- Pegando a Data da Informacao e Gravando na Tabela -------------------
    import requests
    from bs4 import BeautifulSoup
    import re
    import dask.dataframe as dd
    
    if exec_lake == 'prod':
        print('CETUS --> Executando em Producao')
        extr = 'P'

        # Utilizado para producao (execucao semanal ou mensal)
        URL = 'http://www.portaltransparencia.gov.br/beneficios/consulta?ordenarPor=mesAno&direcao=desc'
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, 'html.parser')
        tableDiv = soup.find_all('div', id="datas")
        datinfo = re.sub("[^\d\.]", "", str(tableDiv))[26:]
        print('Data da Info: ', datinfo)
        
        #Data baixar arquivo
        datinfo_mes = datinfo[2:-4]
        datinfo_ano = datinfo[4:]
        ANOMES_INFO = datinfo_ano+datinfo_mes
        print('Data para baixar arquivo:',ANOMES_INFO)
        # ----------------------------------------------------------------------------
    else:
        print('CETUS --> Executando Carga Historica')
        # Formato que pegamos no site quando no modo producao (DDMMAAAA)
        datinfo = str(ANOMES_INFO)
        extr = 'H'
 
   
    var_url = 'http://www.portaltransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/'+str(ANOMES_INFO)
    print('URL para download dos dados:')
    print(var_url)

    print('Baixando dados...')
    
    #----- DEV ------
    #import urllib.request
    #urllib.request.urlretrieve(var_url, 'dados_bolsafam_' + str(ANOMES_INFO) + '.zip')
 
    #---- PROD ------
    import urllib 
    urllib.urlretrieve(var_url, 'dados_bolsafam_' + str(ANOMES_INFO) + '.zip')   
    #----------------

    print('Descompactando dados...')
    import zipfile
    with zipfile.ZipFile('dados_bolsafam_' + str(ANOMES_INFO) + '.zip',"r") as zip_ref:
        zip_ref.extractall('dados_bolsafam_' + str(ANOMES_INFO))
    print('Arquivos disponiveis') 

    #-----------------------  Bolsa Familia ------------------------------
    # Verificar este nome pelo arquivo descompactado 
    nm_file1 = str(ANOMES_INFO)+'_BolsaFamilia_Pagamentos.csv'

    #-----------------------  Bolsa Familia ------------------------------
    import pandas as pd
    import chardet
    print('Decodificando dados...')
    with open('dados_bolsafam_' + str(ANOMES_INFO) +'/' + nm_file1, 'rb') as f:
        result = chardet.detect(f.readline())  # or readline if the file is large
    print('Carregando dados em memoria...')
    df_00 = pd.read_csv('dados_bolsafam_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    encode_df = result['encoding']
    df_00['DT_INFO'] = ANOMES_INFO
    df_00['ID_PROC'] = extr
    return df_00,encode_df,datinfo

#names=["a", "b"], dtype={"a": np.str, "b": np.float64}

def CetusSalvaCSVS3(dataframe,nm_s3_file,s3_path,encode_df):

    import s3fs
    S3fs = s3fs.S3FileSystem()

    # Nome do arquivo a ser salvo no S3
    #nm_s3_file = 'OrcamentoDespesas_'+str(dtref)+'.csv'

    # Caminho no lake (S3)
    #s3_path = 'projeto-cetus/datalake/OrcamentoDespesas/dados/'

    bytes_to_write = dataframe.to_csv(sep = ',',header=True, index=False, index_label=None,encoding=encode_df).encode()
    with S3fs.open(s3_path+nm_s3_file, 'wb') as f:
        f.write(bytes_to_write)
      
        
def CetusRefVecDays(inicio,fim):
     import datetime
     start = datetime.datetime.strptime(inicio, "%Y%m%d")
     end = datetime.datetime.strptime(fim, "%Y%m%d")
     date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
     #print('Data inicial: ',start.strftime("%Y%m%d") ) 
     #print('Data final: ',end.strftime("%Y%m%d") ) 
     #print('Quantidade de Partições: ',len(date_generated)) 
     return date_generated

##############################################################################


################################################################################################
#------------------------ FIM FUNCOES ----------------------------------------------------------    

#------------------------ Executando PRODUCAO ---------------------------------------------------
import datetime
import time
now = datetime.datetime.now() - datetime.timedelta(0,0,0,0,3)
anomesdia = now.strftime("%Y%m%d")
diamesano = now.strftime("%d%m%Y")
anoref = now.strftime("%Y")

start_time = time.clock()
print('BC_Prod_BolFamPag')
print('Prod - Carga de referencia: ',anomesdia)
dados,encode_df,datinfo = CetusLakeBeneficiosCidadaoBolsaFamilia(diamesano,anoref,exec_lake = 'prod')
print('Prod - Data info: ',datinfo)
nm_s3_file = 'BC_Lake_BolFamPag_'+str(datinfo)+'.csv'
s3_path = 'projeto-bigdata-cetus/datalake/BeneficiosCidadaos/'
print('Salvando tabela no lake ---> ',nm_s3_file)
CetusSalvaCSVS3(dados,nm_s3_file,s3_path,encode_df)
print('Carga Efetuada Com Sucesso')
fin_time = time.clock()
delta = round(((fin_time-start_time)/60),4)
print('Tempo(min) para extracao ----> ',delta)

################################################################################################
#------------------------ Executando Carga Historica -------------------------------------------    
# import datetime
# import time

# now = datetime.datetime.now() - datetime.timedelta(0,0,0,0,3)
# anomesdia = now.strftime("%Y%m%d")
# diamesano = now.strftime("%d%m%Y")
# anoref = now.strftime("%Y")

# vetor_safras_aux = CetusRefVecDays('20160101','20181231') 
# #s3_path = 'projeto-bigdata-cetus/datalake/xxxxxxxxxxxx/'

# anomes = []
# for data_ref in vetor_safras_aux:    
#     anomes.append(data_ref.strftime("%Y%m"))
    
# vetor_safras = list(dict.fromkeys(anomes))


# for anomes in vetor_safras:
#     start_time = time.clock()
#     print('Cetus - Carga Histórica Ref ---->', anomes)
#     dados,encode_df,datinfo = CetusLakeBeneficiosCidadaoBolsaFamilia(diamesano,anomes,exec_lake = 'Hist')
#     nm_s3_file = 'BC_Lake_BolFamPag_'+str(datinfo)+'.csv'
#     s3_path = 'projeto-bigdata-cetus/datalake/BeneficiosCidadaos/'
#     print('Salvando tabela no lake ---> ',nm_s3_file)
#     CetusSalvaCSVS3(dados,nm_s3_file,s3_path,encode_df)
#     fin_time = time.clock()
#     delta = round(((fin_time-start_time)/60),4)
#     print('Tempo(min) para extracao ----> ',delta)

################################ FIM ###########################################################