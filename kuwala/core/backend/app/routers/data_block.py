from database.database import get_db
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

router = APIRouter(
    prefix="/block/data",
    tags=["data_block"],
)


@router.post("/")
def create_data_block(db: Session = Depends(get_db)):
    return "yes"
