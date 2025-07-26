from sqlmodel import Session, select
import os
import pandas as pd
from models import Marca, Modelo, Ano
from create_db import create_db_and_tables, engine
from dotenv import load_dotenv
from log import logs

load_dotenv()

def inserir_anos():
    df = verifica_anos_db()
    if df.empty:
        logger.warning("⚠️  Não há novos registros de anos para inserir no banco de dados.")
        return None
    else:
        df.sort_values(by=['year', 'brand_code', 'model_code'], inplace=True)
        logger.info(f"✅ Inserindo {len(df)} novos registros de anos no banco de dados.")
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
            logger.info(f"✅ {len(anos)} registros de anos foram inseridos no banco de dados.")
        return anos

def verifica_anos_db():
    with Session(engine) as session:
        statement = select(Ano.id_modelo, Ano.ano)
        consulta = session.exec(statement)
        df_anos_db = pd.DataFrame(consulta.all())
        if not df_anos_db.empty:
            df_anos_db.columns = ['id_modelo', 'ano']
            df_anos_db['ano'] = df_anos_db['ano'].astype('int64')
            lista_ano_db = list(zip(df_anos_db['id_modelo'], df_anos_db['ano']))
        else:
            lista_ano_db = []

    df_anos = pd.read_csv(f"{data_path}/anos.csv")
    lista_ano_csv = df_anos[['model_code', 'year' ]]
    # lista_ano_csv.columns = ['id_modelo', 'ano']
    # Converte o DataFrame df_anos em uma lista de tuplas
    lista_ano_csv = list(zip(lista_ano_csv['model_code'], lista_ano_csv['year']))
    lista_diff = [item for item in lista_ano_csv if item not in lista_ano_db]

    df_anos['model_code'] = df_anos['model_code'].astype(int)
    df_anos['year'] = df_anos['year'].astype(int)
    # Cria um DataFrame temporário com os valores de diff
    df_diff = pd.DataFrame(lista_diff, columns=['model_code', 'year'])
    # Faz o merge para filtrar apenas as linhas presentes em diff
    df_filtrado = pd.merge(df_anos, df_diff, on=['model_code', 'year'])
    return df_filtrado

def verifica_modelos_db():
    with Session(engine) as session:
        statement = select(Modelo.id_modelo)
        result = session.exec(statement)
        resultado = pd.DataFrame(result.all())
        if not resultado.empty:
            resultado.columns = ['id_modelo']
        else:
            resultado = []

    if hasattr(resultado, 'values'):
        lista_resultado = resultado['id_modelo'].tolist()
    else:
        lista_resultado = list(resultado)

    modelos_csv = pd.read_csv(f"{data_path}/modelos.csv")
    list_modelos = modelos_csv['code'].tolist()
    list_diff = [item for item in list_modelos if item not in lista_resultado]

    novos_modelos = modelos_csv[modelos_csv['code'].isin(list_diff)]

    return novos_modelos

def inserir_modelos():
    df = verifica_modelos_db()
    if df.empty:
        logger.warning("⚠️  Não há novos registros de modelos para inserir no banco de dados.")
        return None
    else:
        df.sort_values(by='code', inplace=True)
        logger.info(f"✅ Inserindo {len(df)} registros de modelos no banco de dados.")
        modelos = [
            Modelo(
                id_modelo=int(row['code']),
                nome = str(row['name']),
                id_marca=int(row['brand_code'])
            )
            for _, row in df.iterrows()
        ]
        with Session(engine) as session:
            session.add_all(modelos)
            session.commit()
            logger.info(f"✅ {len(modelos)} registros de modelos foram inseridos no banco de dados.")
        return modelos

def verifica_marcas_db():
    with Session(engine) as session:
        statement = select(Marca.id_marca)
        result = session.exec(statement)
        resultado = pd.DataFrame(result.all())
        if not resultado.empty:
            resultado.columns = ['id_marca']
        else:
            resultado = []

    if hasattr(resultado, 'values'):
        lista_marcas_db = resultado['id_marca'].tolist()
    else:
        lista_marcas_db = list(resultado)

    marcas_csv = pd.read_csv(f"{data_path}/marcas.csv")
    lista_marcas_csv = marcas_csv['code'].tolist()
    list_diff = [item for item in lista_marcas_csv if item not in lista_marcas_db]

    novas_marcas = marcas_csv[marcas_csv['code'].isin(list_diff)]
    return novas_marcas

def inserir_marcas():
    df = verifica_marcas_db()
    if df.empty:
        logger.warning("⚠️  Não há novos registros de marcas para inserir no banco de dados.")
        return None 
    else:
        df.sort_values(by='code', inplace=True)
        logger.info(f"✅ Inserindo {len(df)} registros de marcas no banco de dados.")
        marcas = [
            Marca(
                id_marca=int(row['code']),
                nome = str(row['name'])
            )
            for _, row in df.iterrows()
        ]
        with Session(engine) as session:
            session.add_all(marcas)
            session.commit()
            logger.info(f"✅ {len(marcas)} registros de marcas foram inseridos no banco de dados.")
            return marcas

if __name__ == "__main__":
    data_path = os.getenv("PROJECT_PATH", "None") + "data"
    log_path = os.getenv("PROJECT_PATH", "None") + "logs"

    logger = logs(f"{log_path}/logs.log", "logger_crud")

    if not os.path.exists("database.db"):
        create_db_and_tables()

    inserir_marcas()
    inserir_modelos()
    inserir_anos()



    
    