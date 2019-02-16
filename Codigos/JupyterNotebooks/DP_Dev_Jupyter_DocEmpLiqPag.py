# Autor: Bruno Jardim
# Data: 10/02/2019
# Descrição: Código para salvar os dados do portal transparência no datalake (AWS - S3)
# URL: http://www.portaltransparencia.gov.br/download-de-dados

ANOMES_INFO = 20130331
import urllib.request
urllib.request.urlretrieve('http://www.portaltransparencia.gov.br/download-de-dados/despesas/'+str(ANOMES_INFO), 'dados_despesaspub_' + str(ANOMES_INFO) + '.zip')

import zipfile
with zipfile.ZipFile('dados_despesaspub_' + str(ANOMES_INFO) + '.zip',"r") as zip_ref:
    zip_ref.extractall('dados_despesaspub_' + str(ANOMES_INFO))




nm_file1 = str(ANOMES_INFO)+'_Despesas_Empenho.csv'
nm_file2 = str(ANOMES_INFO)+'_Despesas_ItemEmpenho.csv'
nm_file3 = str(ANOMES_INFO)+'_Despesas_Liquidacao.csv'
nm_file4 = str(ANOMES_INFO)+'_Despesas_Liquidacao_EmpenhosImpactados.csv'
nm_file5 = str(ANOMES_INFO)+'_Despesas_Pagamento.csv'
nm_file6 = str(ANOMES_INFO)+'_Despesas_Pagamento_EmpenhosImpactados.csv'
nm_file7 = str(ANOMES_INFO)+'_Despesas_Pagamento_ListaFaturas.csv'
nm_file8 = str(ANOMES_INFO)+'_Despesas_Pagamento_FavorecidosFinais.csv'
nm_file9 = str(ANOMES_INFO)+'_Despesas_Pagamento_ListaBancos.csv'
nm_file10 = str(ANOMES_INFO)+'_Despesas_Pagamento_ListaPrecatorios.csv'



#-----------------------  EMPENHO ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_empenho = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_empenho['DT_INFO'] = ANOMES_INFO
df_empenho['PK_NMR_DPS'] = df_empenho['Id Empenho']


#-----------------------  ITEM EMPENHO ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file2, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_iempenho = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_iempenho['DT_INFO'] = ANOMES_INFO
df_iempenho['PK_NMR_DPS'] = df_iempenho['Id Empenho']


#-----------------------  LIQUIDAÇÃO  ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file3, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_liquidacao = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_liquidacao['DT_INFO'] = ANOMES_INFO
df_liquidacao['PK_NMR_DPS'] = df_liquidacao['Código Liquidação']


#-----------------------  LIQUIDAÇÃO EMPENHOS IMPACTADOS ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file4, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_liquidacaoemp = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_liquidacaoemp['DT_INFO'] = ANOMES_INFO
df_liquidacaoemp['PK_NMR_DPS'] = df_liquidacaoemp['Código Liquidação']


#-----------------------  PAGAMENTO ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file5, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_pagamento = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_pagamento['DT_INFO'] = ANOMES_INFO
df_pagamento['PK_NMR_DPS'] = df_pagamento['Código Pagamento']


#-----------------------  PAGAMENTO EMPENHOS IMPACTADOS ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file6, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_pagamentoemp = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_pagamentoemp['DT_INFO'] = ANOMES_INFO
df_pagamentoemp['PK_NMR_DPS'] = df_pagamentoemp['Código Pagamento']



#-----------------------  PAGAMENTO LISTA FATURAS ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file7, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_pagamentolistas = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_pagamentolistas['DT_INFO'] = ANOMES_INFO
df_pagamentolistas['PK_NMR_DPS'] = df_pagamentolistas['Código Pagamento']


#-----------------------  PAGAMENTO FAVORECIDOS FINAIS ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file8, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_pagamentofav = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_pagamentofav['DT_INFO'] = ANOMES_INFO
df_pagamentofav['PK_NMR_DPS'] = df_pagamentofav['Código Pagamento']


#-----------------------  PAGAMENTO LISTA BANCOS ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file9, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_pagamentobancos = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_pagamentobancos['DT_INFO'] = ANOMES_INFO
df_pagamentobancos['PK_NMR_DPS'] = df_pagamentobancos['Código Pagamento']


#-----------------------  PAGAMENTO LISTA PRECATÓRIOS ------------------------------
import pandas as pd
import chardet
with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file10, 'rb') as f:
    result = chardet.detect(f.read())  # or readline if the file is large
df_pagamentoprecatorios = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
df_pagamentoprecatorios['DT_INFO'] = ANOMES_INFO
df_pagamentoprecatorios['PK_NMR_DPS'] = df_pagamentoprecatorios['Código Pagamento']

df_empenho.head()

df_iempenho.head()

df_liquidacao.head()

df_liquidacaoemp.head()

df_pagamento.head()

df_pagamentoemp.head()

df_pagamentolistas.head()

df_pagamentofav.head()

df_pagamentobancos.head()

df_pagamentoprecatorios.head()


df_empenho.shape, df_iempenho.shape, df_liquidacao.shape, df_liquidacaoemp.shape, df_pagamento.shape, df_pagamentoemp.shape, df_pagamentolistas.shape, df_pagamentofav.shape, df_pagamentobancos.shape, df_pagamentoprecatorios.shape




# Função pra pegar várias datas

def CetusContratacoesGov(ANOMES_INFO):
    #ANOMES_INFO = 20130331
    import urllib.request
urllib.request.urlretrieve('http://www.portaltransparencia.gov.br/download-de-dados/despesas/'+str(ANOMES_INFO), 'dados_despesaspub_' + str(ANOMES_INFO) + '.zip')

    import zipfile
    with zipfile.ZipFile('dados_despesaspub_' + str(ANOMES_INFO) + '.zip',"r") as zip_ref:
        zip_ref.extractall('dados_despesaspub_' + str(ANOMES_INFO))  
    
    nm_file1 = str(ANOMES_INFO)+'_Despesas_Empenho.csv'
    nm_file2 = str(ANOMES_INFO)+'_Despesas_ItemEmpenho.csv'
    nm_file3 = str(ANOMES_INFO)+'_Despesas_Liquidacao.csv'
    nm_file4 = str(ANOMES_INFO)+'_Despesas_Liquidacao_EmpenhosImpactados.csv'
    nm_file5 = str(ANOMES_INFO)+'_Despesas_Pagamento.csv'
    nm_file6 = str(ANOMES_INFO)+'_Despesas_Pagamento_EmpenhosImpactados.csv'
    nm_file7 = str(ANOMES_INFO)+'_Despesas_Pagamento_ListaFaturas.csv'
    nm_file8 = str(ANOMES_INFO)+'_Despesas_Pagamento_FavorecidosFinais.csv'
    nm_file9 = str(ANOMES_INFO)+'_Despesas_Pagamento_ListaBancos.csv'
    nm_file10 = str(ANOMES_INFO)+'_Despesas_Pagamento_ListaPrecatorios.csv'

    #-----------------------  EMPENHO ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_empenho = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_empenho['DT_INFO'] = ANOMES_INFO
    df_empenho['PK_NMR_DPS'] = df_empenho['Id Empenho']


    #-----------------------  ITEM EMPENHO ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file2, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_iempenho = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_iempenho['DT_INFO'] = ANOMES_INFO
    df_iempenho['PK_NMR_DPS'] = df_iempenho['Id Empenho']


    #-----------------------  LIQUIDAÇÃO  ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file3, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_liquidacao = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_liquidacao['DT_INFO'] = ANOMES_INFO
    df_liquidacao['PK_NMR_DPS'] = df_liquidacao['Código Liquidação']


    #-----------------------  LIQUIDAÇÃO EMPENHOS IMPACTADOS ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file4, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_liquidacaoemp = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_liquidacaoemp['DT_INFO'] = ANOMES_INFO
    df_liquidacaoemp['PK_NMR_DPS'] = df_liquidacaoemp['Código Liquidação']


    #-----------------------  PAGAMENTO ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file5, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_pagamento = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_pagamento['DT_INFO'] = ANOMES_INFO
    df_pagamento['PK_NMR_DPS'] = df_pagamento['Código Pagamento']


    #-----------------------  PAGAMENTO EMPENHOS IMPACTADOS ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file6, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_pagamentoemp = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_pagamentoemp['DT_INFO'] = ANOMES_INFO
    df_pagamentoemp['PK_NMR_DPS'] = df_pagamentoemp['Código Pagamento']



    #-----------------------  PAGAMENTO LISTA FATURAS ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file7, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_pagamentolistas = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_pagamentolistas['DT_INFO'] = ANOMES_INFO
    df_pagamentolistas['PK_NMR_DPS'] = df_pagamentolistas['Código Pagamento']


    #-----------------------  PAGAMENTO FAVORECIDOS FINAIS ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file8, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_pagamentofav = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_pagamentofav['DT_INFO'] = ANOMES_INFO
    df_pagamentofav['PK_NMR_DPS'] = df_pagamentofav['Código Pagamento']


    #-----------------------  PAGAMENTO LISTA BANCOS ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file9, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_pagamentobancos = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_pagamentobancos['DT_INFO'] = ANOMES_INFO
    df_pagamentobancos['PK_NMR_DPS'] = df_pagamentobancos['Código Pagamento']


    #-----------------------  PAGAMENTO LISTA PRECATÓRIOS ------------------------------
    import pandas as pd
    import chardet
    with open('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file10, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    df_pagamentoprecatorios = pd.read_csv('dados_despesaspub_' + str(ANOMES_INFO) +'/' + nm_file1, encoding=result['encoding'], error_bad_lines=False,sep=';', engine='python')
    df_pagamentoprecatorios['DT_INFO'] = ANOMES_INFO
    df_pagamentoprecatorios['PK_NMR_DPS'] = df_pagamentoprecatorios['Código Pagamento']
 
    
    return df_empenho, df_iempenho, df_liquidacao, df_liquidacaoemp, df_pagamento, df_pagamentoemp, df_pagamentolistas, df_pagamentofav, df_pagamentobancos, df_pagamentoprecatorios




a,b,c,d,e,f,g,h,i,j = CetusContratacoesGov(20130331)

a.head()



def CetusRefVecDays(inicio,fim):
     import datetime
     start = datetime.datetime.strptime(inicio, "%Y%m%d")
     end = datetime.datetime.strptime(fim, "%Y%m%d")
     date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
     #print('Data inicial: ',start.strftime("%Y%m%d") ) 
     #print('Data final: ',end.strftime("%Y%m%d") ) 
     #print('Quantidade de Partições: ',len(date_generated)) 
     return date_generated

def CetusSalvaCSVS3(dataframe,nm_s3_file,s3_path):

    import s3fs
    S3fs = s3fs.S3FileSystem()

    # Nome do arquivo a ser salvo no S3
    #nm_s3_file = 'OrcamentoDespesas_'+str(dtref)+'.csv'

    # Caminho no lake (S3)
    #s3_path = 'projeto-cetus/datalake/OrcamentoDespesas/dados/'

    bytes_to_write = dataframe.to_csv(sep = ',',header=True, index=False, index_label=None).encode()
    with S3fs.open(s3_path+nm_s3_file, 'wb') as f:
        f.write(bytes_to_write)
    return print('Salvo com Sucesso :)')



def CetusReadS3CSVFile(nm_bucket,path_file,sep=','):
    import boto3
    import pandas as pd
    bucket=nm_bucket # Or whatever you called your bucket
    data_key = path_file # Where the file is within your bucket
    data_location = 's3://{}/{}'.format(bucket, data_key)
    try:
        df = pd.read_csv(data_location,sep=sep,encoding='utf-8',error_bad_lines=False) 

    except:
        df = pd.read_csv(data_location,sep=sep,encoding='latin-1',error_bad_lines=False)

    return df



# Carga histórica

vetor_safras_aux = CetusRefVecDays("20130331","20190213") 
s3_path = 'projeto-bigdata-cetus/datalake/DespesasPublicas/'


anomes = []
for data_ref in vetor_safras_aux:    
    anomes.append(data_ref.strftime("%Y%m%d"))
vetor_safras = list(dict.fromkeys(anomes))

for anomes in vetor_safras:
    print('Cetus - Carga Histórica Ref ---->', anomes)
    a,b,c,d,e,f,g,h,i,j = CetusContratacoesGov(anomes)
    
    nm_s3_file_a = 'DP_Lake_DocEmpLiqPag_Empenho_'+str(anomes)+'.csv'
    CetusSalvaCSVS3(a,nm_s3_file_a,s3_path)
    
    nm_s3_file_b = 'DP_Lake_DocEmpLiqPag_Empenho_Item_'+str(anomes)+'.csv'
    CetusSalvaCSVS3(b,nm_s3_file_b,s3_path)
    
    nm_s3_file_c = 'DP_Lake_DocEmpLiqPag_Liquidacao'+str(anomes)+'.csv'
    CetusSalvaCSVS3(c,nm_s3_file_c,s3_path)

    nm_s3_file_d = 'DP_Lake_DocEmpLiqPag_Liquidacao_Empenho'+str(anomes)+'.csv'
    CetusSalvaCSVS3(d,nm_s3_file_d,s3_path)

    nm_s3_file_e = 'DP_Lake_DocEmpLiqPag_Pagamento'+str(anomes)+'.csv'
    CetusSalvaCSVS3(e,nm_s3_file_e,s3_path)

    nm_s3_file_f = 'DP_Lake_DocEmpLiqPag_Pagamento_Empenho'+str(anomes)+'.csv'
    CetusSalvaCSVS3(f,nm_s3_file_f,s3_path)

    nm_s3_file_g = 'DP_Lake_DocEmpLiqPag_Pagamento_Listas'+str(anomes)+'.csv'
    CetusSalvaCSVS3(g,nm_s3_file_g,s3_path)

    nm_s3_file_h = 'DP_Lake_DocEmpLiqPag_Pagamento_Favorecidos'+str(anomes)+'.csv'
    CetusSalvaCSVS3(h,nm_s3_file_h,s3_path)

    nm_s3_file_i = 'DP_Lake_DocEmpLiqPag_Pagamento_Listas_Bancos'+str(anomes)+'.csv'
    CetusSalvaCSVS3(i,nm_s3_file_i,s3_path)

    nm_s3_file_j = 'DP_Lake_DocEmpLiqPag_Pagamento_Listas_Precatórios'+str(anomes)+'.csv'
    CetusSalvaCSVS3(j,nm_s3_file_j,s3_path)