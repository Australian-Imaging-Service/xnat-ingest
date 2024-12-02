from pathlib import Path

from sqlalchemy import create_engine

from viewer.models.metadata import SelectedMetaDataRecord

current_path = Path(__file__).parent
db_path = current_path.joinpath("app.db").absolute()


engine = create_engine(f"sqlite:///{db_path}")
SelectedMetaDataRecord.metadata.create_all(engine)
