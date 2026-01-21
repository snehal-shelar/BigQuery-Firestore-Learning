from fastapi import FastAPI, HTTPException, status

from db_config import db_client
from models import CreatePatient

app = FastAPI()


@app.post("/create-patient", tags=["patient"])
def create_patients(request: CreatePatient):
    """Create patient's records in patient_simulated_data collection."""

    try:
        db_client.collection("patient_simulated_data").add(request.__dict__)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return True


@app.get("/patients", tags=["patient"])
def get_patients():
    """Read the patient's collection."""

    patients = db_client.collection("patient_simulated_data")
    records = patients.stream()
    response_data = list()

    for record in records:
        response_data.append({record.id: record.to_dict()})

    return response_data


@app.patch("/update-patient/{patient_id}", tags=["patient"])
def update_patients(request: CreatePatient, patient_id: str):
    """Update the patient's information."""

    try:
        db_client.collection("patient_simulated_data").document(patient_id).update(
            request.__dict__
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return True


@app.delete("/delete-patient/{patient_id}", tags=["patient"])
def delete_patient(patient_id: str):
    """ Delete record from the patient's collection. """
    try:
        db_client.collection("patient_simulated_data").document(patient_id).delete()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
