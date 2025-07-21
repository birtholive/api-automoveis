from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class Marca(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    id_marca: int
    nome: str
    created_at: datetime = Field(default_factory=datetime.now)

class Modelo(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    id_modelo: int 
    nome: str
    id_marca: int
    created_at: datetime = Field(default_factory=datetime.now)

class Ano(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    id_modelo: int
    id_marca: int
    ano: str
    combustivel: str
    created_at: datetime = Field(default_factory=datetime.now)

