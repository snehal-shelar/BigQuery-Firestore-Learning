from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


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
