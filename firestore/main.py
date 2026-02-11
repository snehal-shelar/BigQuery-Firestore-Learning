from fastapi import FastAPI, HTTPException, status
from google.cloud.firestore import FieldFilter
from google.cloud.firestore_v1.base_query import And, Or

from db_config import db_client
from models import CreatePatient

app = FastAPI()


@app.post("/create-patient", tags=["patient"])
def create_patients(request: CreatePatient):
    """
    1. Create new project in firestore console.
    2. Create new firestore database for newly created project in firestore.
    3. Create collection for new database.
    4. Perform CRUD operation for new collection.
        - Explored followings:
            1. Add records using add() and set() methods.
            2. Get records using get() method.
            3. Update records using update() and set() methods.
            4. Delete records using delete() method.

    Create patient's records in patient_simulated_data collection.
    """

    try:
        breakpoint()
        db_client.collection("patient_simulated_data").add(request.__dict__)

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return True


@app.get("/patients", tags=["patient"])
def get_patients():
    """Read the patient's collection."""

    try:
        patients = db_client.collection("patient_simulated_data")
        records = patients.stream()
        response_data = list()
        for record in records:
            response_data.append({record.id: record.to_dict()})
        return response_data
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.patch("/update-patient/{patient_id}", tags=["patient"])
def update_patients(request: CreatePatient, patient_id: str):
    """Update the patient's information."""

    try:
        import datetime
        request.__dict__["updated_at"] = datetime.datetime.now()
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


@app.get("/patients/search", tags=["search_patients"])
def search_patients(blood_group: str = None, min_age: int = 0):
    """
    Practice Compound Queries (AND/OR logic).
    """
    try:
        collection_ref = db_client.collection("patient_simulated_data")
        # Example of an 'AND' query: Specific blood group AND above a certain age
        # Note: This may require a Composite Index (check terminal for the link)
        query = collection_ref.where(
            filter=And(filters=[
                FieldFilter("blood_group", "==", "A+"),
                FieldFilter("age", ">=", min_age)
            ])
        )

        results = query.stream()
        return [{res.id: res.to_dict()} for res in results]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
