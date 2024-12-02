from sqlalchemy import String
from sqlalchemy.orm import mapped_column, Mapped

from viewer.core.model import BaseModel
from viewer.core.db import DatabaseRecord


class HomeDataRecord(DatabaseRecord):
    __tablename__ = "selected_metadata"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(30))
    path: Mapped[str] = mapped_column(String(250))
    key: Mapped[str] = mapped_column(String(250))
    value: Mapped[str] = mapped_column(String(250))


class HomeModel(BaseModel):
    def __init__(self):
        super().__init__(record_type=HomeDataRecord)
