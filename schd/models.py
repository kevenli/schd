from sqlmodel import SQLModel, Field
from typing import Optional

class JobModel(SQLModel, table=True):
    __tablename__ = 'jobs'
    
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    active: bool
    cron: Optional[str]
    worker_id: Optional[str]
