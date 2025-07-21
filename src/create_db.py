from sqlmodel import create_engine, SQLModel
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

engine = create_engine("sqlite:///data/database.db")

def create_db_and_tables():
    try:
        SQLModel.metadata.create_all(engine)
        logging.info(f"✅ Banco de dados criado com sucesso")
    except Exception as e:
        logging.error(f"❌ Erro ao criar o banco de dados")
        raise # significa que o erro será propagado para o nível superior