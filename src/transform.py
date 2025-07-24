import pandas as pd
import json
from dotenv import load_dotenv
import os
from log import logs

load_dotenv()

def transform_anos(data_path):
    df = pd.read_csv(f"{data_path}/anos.csv")

    # Parseando a string JSON
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
    df_final.to_csv(f"{data_path}/anos_transformados.csv", index=False)
    return df_final

if __name__ == "__main__":
    data_path = os.getenv("PROJECT_PATH", "None") + "data"
    log_path = os.getenv("PROJECT_PATH", "None") + "logs"

    logger = logs(f"{log_path}/transform.log", "logger_transform")

    logger.info("Iniciando transformação de anos")
    dados_transformados = transform_anos(data_path)
    
    logger.info("Gravando dados transformados em CSV")
    dados_transformados.to_csv(f"{data_path}/anos_transformados.csv", index=False)