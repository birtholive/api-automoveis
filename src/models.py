from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class Marca(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    nome: str

class Modelo(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    nome: str
    id_marca: int

class Ano(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    id_modelo: int
    id_marca: int
    ano: str
    combustivel: str
    created_at: datetime = Field(default_factory=datetime.now)

