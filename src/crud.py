from sqlmodel import SQLModel, Session, create_engine, select
import os
import logging
import pandas as pd
import numpy as np
from models import Marca, Modelo, Ano

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

engine = create_engine("sqlite:///src/database.db")


def create_db_and_tables():
    try:
        SQLModel.metadata.create_all(engine)
        logging.info(f"Banco de dados criado com sucesso")
    except Exception as e:
        logging.error(f"Erro ao criar o banco de dados")
        raise # significa que o erro será propagado para o nível superior

def inserir_ano(df):
    anos = [
        Ano(
            id_modelo=int(row['model_code']),
            id_marca=int(row['brand_code']),
            ano=str(row['year']),
            combustivel=str(row['fuel'])
        )
        for _, row in df.iterrows()
    ]
    with Session(engine) as session:
        session.add_all(anos)
        session.commit()
        print(f"{len(anos)} linhas foram inseridas no banco de dados.")
    return anos

def consulta_anos_db():
    with Session(engine) as session:
        statement = select(Ano.id_modelo, Ano.ano)
        result = session.exec(statement)
        resultado = pd.DataFrame(result.all())
        if not resultado.empty:
            resultado.columns = ['id_modelo', 'ano']
            resultado['ano'] = resultado['ano'].astype('int64')
            resultado = list(zip(resultado['id_modelo'], resultado['ano']))
        else:
            resultado = []
        return resultado

def consulta_anos_csv():
    df_anos = pd.read_csv(f"{data_path}/anos_transformados.csv")
    df_anos = df_anos[['model_code', 'year' ]]
    df_anos.columns = ['id_modelo', 'ano']
    # Converte o DataFrame df_anos em uma lista de tuplas
    anos_csv = list(zip(df_anos['id_modelo'], df_anos['ano']))
    return anos_csv

def filtra_anos_csv(diff):
    df_anos = pd.read_csv(f"{data_path}/anos_transformados.csv")
    # Filtra as linhas do df_anos que contenham os dados da lista diff para as colunas model_code e year respectivamente
    # diff é uma lista de tuplas (id_modelo, ano)
    # Precisamos garantir que as colunas estejam no tipo correto para comparação
    df_anos['model_code'] = df_anos['model_code'].astype(int)
    df_anos['year'] = df_anos['year'].astype(int)
    # Cria um DataFrame temporário com os valores de diff
    df_diff = pd.DataFrame(diff, columns=['model_code', 'year'])
    # Faz o merge para filtrar apenas as linhas presentes em diff
    df_filtrado = pd.merge(df_anos, df_diff, on=['model_code', 'year'])
    return df_filtrado


if __name__ == "__main__":
    data_path = os.getenv("PROJECT_PATH", "None") + "data"
    # create_db_and_tables()
    # df_marcas = pd.read_csv(f"{data_path}/marcas.csv")
    # df_modelos = pd.read_csv(f"{data_path}/modelos.csv")
    # df_anos = pd.read_csv(f"{data_path}/anos_transformados.csv")
    # Para verificar se o banco de dados SQLite existe, podemos checar se o arquivo existe no sistema de arquivos.
    if not os.path.exists("database.db"):
        create_db_and_tables()

    anos_db = consulta_anos_db()
    anos_csv = consulta_anos_csv()
    diff = [item for item in anos_csv if item not in anos_db]
    diff_filtrado = filtra_anos_csv(diff)
    if not diff_filtrado.empty:
        inserir_ano(diff_filtrado)
    else:
        logging.info("Não há diferenças entre o arquivo CSV e do banco de dados")
    
    