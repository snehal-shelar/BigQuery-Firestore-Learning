from pydantic import BaseModel


class CreatePatient(BaseModel):
    """
    Validation model for requested patient data.
    """
    first_name: str 
    last_name: str 
    patient_id: str
    blood_group: str 

