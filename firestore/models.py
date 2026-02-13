import datetime
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, List, Dict


class Settings(BaseSettings):
    FIREBASE_CREDENTIALS_PATH: str
    FIREBASE_APPLICATION_URL: str

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()


class CreatePatient(BaseModel):
    """
    Validation model for requested patient data.
    """
    first_name: str
    last_name: str
    patient_id: str
    blood_group: str
    icd_code: List[str]
    patient_details: Dict
    date_of_birth: str
    billed_amount: float
    # created_at: datetime | None = None
    # updated_at: datetime | None = None


class PaginatedResponse(BaseModel):
    data: List
    next_cursor: Optional[str] = None
    previous_cursor: Optional[str] = None
    has_more: bool = True


class CreateReports(BaseModel):
    name: str
    report_link: str
    patient_id: str
