from sqlmodel import create_engine, SQLModel
from log import logs
from dotenv import load_dotenv
import os

load_dotenv()

log_path = os.getenv("PROJECT_PATH", "None") + "logs"

logger = logs(f"{log_path}/logs.log", "create_db")

engine = create_engine("sqlite:///data/database.db")

def create_db_and_tables():
    try:
        SQLModel.metadata.create_all(engine)
        logger.debug(f"✅ Banco de dados criado com sucesso")
    except Exception as e:
        logger.error(f"❌ Erro ao criar o banco de dados")
        raise # significa que o erro será propagado para o nível superior