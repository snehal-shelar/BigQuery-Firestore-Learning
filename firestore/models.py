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
    {
      "first_name": "Nidhi",
      "last_name": "Jadon",
      "patient_id": "#5",
      "blood_group": "A+",
      "icd_code": [
        "ICD1", "ICD2
      ],
      "date_of_birth": "12-02-1999",
      "patient_details": {
               "city": "Vadodara",
               "state": "Gujarat"
      }
    }
    """
    first_name: str
    last_name: str
    patient_id: str
    blood_group: str
    icd_code: List[str]
    patient_details: Dict
    date_of_birth: str
