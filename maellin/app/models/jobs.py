from pydantic import BaseModel, StrBytes


class Job(BaseModel):
    name: str
    trigger: str
    minutes: int
    max_instances: int
    executor: str
    jobstore:str
    replace_existing: bool
    dag : StrBytes
