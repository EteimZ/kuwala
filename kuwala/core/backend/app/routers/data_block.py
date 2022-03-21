from typing import Optional

import controller.data_block_controller as data_block_controller
from database.database import get_db
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

router = APIRouter(
    prefix="/block/data",
    tags=["data_block"],
)


@router.post("/")
def create_data_block(
    name: str,
    data_source_id: str,
    table_name: str,
    schema_name: str = None,
    dataset_name: str = None,
    columns: Optional[list[str]] = Query(None),
    db: Session = Depends(get_db),
):
    return data_block_controller.create_data_block(
        name=name,
        data_source_id=data_source_id,
        table_name=table_name,
        schema_name=schema_name,
        dataset_name=dataset_name,
        columns=columns,
        db=db,
    )
