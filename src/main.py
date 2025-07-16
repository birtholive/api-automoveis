import pandas as pd
import os
from dotenv import load_dotenv
import requests
import time

load_dotenv()

def extract_marcas():
    df = pd.read_json("https://fipe.parallelum.com.br/api/v2/cars/brands")
    return df

def extract_modelos():
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
        par_ano = df_anos[["model_code", "brand_code"]]
        diff = pd.concat([par_modelo, par_ano]).drop_duplicates(keep=False)
    else:
        diff = df_modelos[["code", "brand_code"]]
        diff.columns = ["model_code", "brand_code"]
    return diff

def extract_anos(diff):
    df_anos = pd.DataFrame()

    for model_code, brand_code in diff.values:
        try:
            response = requests.get(f"https://fipe.parallelum.com.br/api/v2/cars/brands/{brand_code}/models/{model_code}/years")
            response.raise_for_status()   # opcional: levanta exceção em caso de erro HTTP
            texto = response.text        # aqui está todo o conteúdo da resposta em string
            df_temp = pd.DataFrame([{"model_code": model_code, "brand_code": brand_code, "model_ano":texto}])
            df_anos = pd.concat([df_anos, df_temp], ignore_index=True)
        except Exception as e:
            print(f"Ocorreu um erro ao processar o modelo {model_code} da marca {brand_code}. Encerrando o loop para evitar bloqueio por limite de requisições.\nErro: {e}")
            break    
        time.sleep(0.2)  # Pequeno delay para evitar limite de requisições    
    return df_anos

if __name__ == "__main__":
     # Carregar variáveis de ambiente
    data_path = os.getenv("PROJECT_PATH", "None") + "data"

    # df_marcas = extract_marcas()
    # df_marcas.to_csv(f"{data_path}/marcas.csv", index=False)

    # df_modelos = extract_modelos()
    # df_modelos.to_csv(f"{data_path}/modelos.csv", index=False)

    diff = define_diff()
    df_anos = extract_anos(diff)
    df_anos.to_csv(f"{data_path}/anos.csv", index=False, mode="a+", header=False)
    

