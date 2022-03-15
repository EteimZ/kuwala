from sqlalchemy import Column, ForeignKey, String

from ..database import Base


class DataBlock(Base):
    __tablename__ = "data_blocks"

    id = Column(String, primary_key=True, index=True)
    data_source_id = Column(String, ForeignKey("data_sources.id"))
    name = Column(String, nullable=False)
    dbt_model = Column(String, nullable=False)
    dbt_yaml = Column(String, nullable=False)
