import pandas as pd
import os
from dotenv import load_dotenv
import requests
import time
from log import logs
import json
import ast

load_dotenv()

def extract_marcas():
    logger.info("Iniciando extração de marcas")
    df = pd.read_json("https://fipe.parallelum.com.br/api/v2/cars/brands")
    return df

def extract_modelos():
    logger.info("Iniciando extração de modelos")
    df_marcas = pd.read_csv("marcas.csv")
    df_modelos = pd.DataFrame()
    for code in df_marcas["code"]:
        df_temp = pd.read_json(f"https://fipe.parallelum.com.br/api/v2/cars/brands/{code}/models")
        df_temp["brand_code"] = code
        df_modelos = pd.concat([df_modelos, df_temp])
    return df_modelos

def define_diff():
    df_modelos = pd.read_csv(f"{data_path}/modelos.csv")

    if os.path.exists(f"{data_path}/anos.csv"):
        
        df_anos = pd.read_csv(f"{data_path}/anos.csv")

        par_modelo = df_modelos[["code", "brand_code"]]
        par_modelo.columns = ["model_code", "brand_code"]
        par_ano = df_anos[["model_code", "brand_code"]].copy()
        par_ano.drop_duplicates(inplace=True)
        diff = pd.concat([par_modelo, par_ano]).drop_duplicates(keep=False)
    else:
        diff = df_modelos[["code", "brand_code"]]
        diff.columns = ["model_code", "brand_code"]
    logger.info(f"Diferenças encontradas: {len(diff)}")
    return diff

def extract_anos(diff):
    df_anos = pd.DataFrame()
    logger.info(f"Iniciando extração de anos para {len(diff)} modelos")

    for model_code, brand_code in diff.values:
        try:
            response = requests.get(f"https://fipe.parallelum.com.br/api/v2/cars/brands/{brand_code}/models/{model_code}/years")
            response.raise_for_status()   # opcional: levanta exceção em caso de erro HTTP
            texto = response.text        # aqui está todo o conteúdo da resposta em string
            df_temp = pd.DataFrame([{"model_code": model_code, "brand_code": brand_code, "model_ano":texto}])
            df_anos = pd.concat([df_anos, df_temp], ignore_index=True)
        except Exception as e:
            logger.error(f"Ocorreu um erro ao processar o modelo {model_code} da marca {brand_code}")
            logger.error(f"Erro: {e}")
            break    
        time.sleep(0.2)  # Pequeno delay para evitar limite de requisições    
    return df_anos

def transform_anos(df):
    if not df.empty:
        df['model_ano'] = df['model_ano'].apply(json.loads)

        # Expandindo o JSON em linhas separadas
        df_expanded = df.explode('model_ano')

        # Criando colunas a partir das chaves do dicionário
        df_final = pd.concat([
            df_expanded.drop('model_ano', axis=1),
            df_expanded['model_ano'].apply(pd.Series)
        ], axis=1)
        df_final.columns = ['model_code', 'brand_code', 'ano_code', 'ano_name']
        df_final['year'] = df_final['ano_name'].apply(lambda x: x.split()[0])
        df_final['fuel'] = df_final['ano_name'].apply(lambda x: x.split()[1])
        df_final.drop(columns=['ano_code', 'ano_name'], inplace=True)
        logger.info(f"Anos transformados e gravados com sucesso: {len(df_final)}")
    else:
        logger.warning("Não há novos registros de anos para serem tratados.")
        df_final = pd.DataFrame()
    return df_final


def lista_anos():
    dicionario = {"Gasolina":1,"Álcool":2,"Diesel":3}
    anos = pd.read_csv(f'{data_path}/anos.csv')
    anos['fuel'] = anos['fuel'].map(dicionario)
    anos['year'] = anos['year'].astype(str) + '-' + anos['fuel'].astype(str)
    lista_modelo_ano = list(anos[['brand_code', 'model_code', 'year']].itertuples(index=False, name=None))

    if os.path.exists(f"{data_path}/fipe.csv"):
        bd = pd.read_csv(f'{data_path}/fipe.csv', usecols=['marca_modelo_ano'])
        bd=list(bd['marca_modelo_ano'])
        bd = [ast.literal_eval(s) for s in bd]
        lista_diff = [item for item in lista_modelo_ano if item not in bd]
    else:
        lista_diff = lista_modelo_ano
    return lista_diff


def consultaFipe():
    lista = lista_anos()
    registros = len(lista)
    logger.info(f'{registros} itens a serem processados')
    df = pd.DataFrame()
    for (marca, modelo, ano) in lista:
        url = f'https://fipe.parallelum.com.br/api/v2/cars/brands/{marca}/models/{modelo}/years/{ano}'
        response = requests.get(url)
        # response.raise_for_status()
        status = response.status_code
        if status == 200:
            response = response.json()
            df_temp = pd.DataFrame([response])
            df_temp.drop(['vehicleType','brand','model','modelYear','fuel','fuelAcronym'], axis=1)
            df_temp['marca_modelo_ano'] = [(marca, modelo, ano)] * len(df_temp)
            df = pd.concat([df, df_temp], ignore_index=True)
            print(df_temp)
        else:
            logger.error(f'Erro ao processar a url {url}')
            break
    time.sleep(0.2)
    logger.info(f'{len(df)} de {registros} itens foram processados')
    return df

if __name__ == "__main__":
     # Carregar variáveis de ambiente
    data_path = os.getenv("PROJECT_PATH", "None") + "data"
    log_path = os.getenv("PROJECT_PATH", "None") + "logs"

    logger = logs(f"{log_path}/logs.log", "extract")

    # df_marcas = extract_marcas()
    # df_marcas.to_csv(f"{data_path}/marcas.csv", index=False)

    # df_modelos = extract_modelos()
    # df_modelos.to_csv(f"{data_path}/modelos.csv", index=False)

    # diff = define_diff()
    # df_anos = extract_anos(diff)
    # df_anos_transformados = transform_anos(df_anos)
    # if not df_anos_transformados.empty:
    #    df_anos_transformados.to_csv(f"{data_path}/anos.csv", index=False, mode="a+", header=False)

    dados_fipe = consultaFipe()
    dados_fipe.to_csv(f"{data_path}/fipe.csv", index=False, mode="a+", header=False)
       

