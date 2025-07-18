from sqlmodel import SQLModel, Field,Session, create_engine
from sqlalchemy import case
import os
import logging
import pandas as pd
import numpy as np
from models import Marca, Modelo, Ano

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
        print(f"{len(anos)} linhas do DataFrame foram inseridas na tabela Ano do banco de dados.")
    return anos


if __name__ == "__main__":
    data_path = os.getenv("PROJECT_PATH", "None") + "data"
    create_db_and_tables()
    df_marcas = pd.read_csv(f"{data_path}/marcas.csv")
    df_modelos = pd.read_csv(f"{data_path}/modelos.csv")
    df_anos = pd.read_csv(f"{data_path}/anos_transformados.csv")
    inserir_ano(df_anos)
    # inserir_marca(df_marcas)
    # inserir_modelo(df_modelos)