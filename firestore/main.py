import datetime
from fastapi import FastAPI, HTTPException, status, Request
from google.cloud.firestore import FieldFilter
from google.cloud.firestore_v1.base_query import And, Or
from typing import Dict, List

from db_config import db_client
from models import CreatePatient, CreateReports

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
        request.__dict__["created_at"] = datetime.datetime.now()
        db_client.collection("patient_simulated_data").add(request.__dict__)

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return True


@app.get("/patients", tags=["patient"])
def get_patients(last_doc_id: str, per_page: int = 10):
    """Read the patient's collection."""

    try:
        patients_collection = db_client.collection("patient_simulated_data")
        query = patients_collection.order_by("patient_id", direction="DESCENDING").limit(per_page)

        if last_doc_id:
            last_doc = patients_collection.document(last_doc_id).get()
            if last_doc.exists:
                query = query.start_after(last_doc)

        records = query.stream()
        response_data = list()
        last_id = None
        for record in records:
            response_data.append({record.id: record.to_dict()})
            last_id = record.id

        return {
            "data": response_data,
            "next_cursor_id": last_id,  # Client sends this back as 'last_doc_id' for page 2
            "has_more": len(response_data) == per_page
        }

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.patch("/update-patient/{patient_id}", tags=["patient"])
def update_patients(request: CreateReports, patient_id: str):
    """Update the patient's information."""

    try:
        request.__dict__["updated_at"] = datetime.datetime.now()
        # db_client.collection("patient_simulated_data").document(patient_id).update(
        #     request.__dict__
        # )

        doc_id = db_client.collection("patient_simulated_data").document(patient_id)
        attach_reports = doc_id.collection("reports").document("report1")
        attach_reports.set(request.__dict__)

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return True


@app.patch("/bulk-update", tags=["bulk-update-patients"])
def bulk_update():
    """
        This method is used to bulk update the patients' information using batch.
        firestore has limitation on bulk update.
    """
    try:
        patient_collection = db_client.collection("patient_simulated_data")
        batch_init = db_client.batch()
        documents = patient_collection.stream()
        count = 0
        for doc in documents:
            batch_init.update(doc.reference,
                              {"created_at": datetime.datetime.now(), "updated_at": datetime.datetime.now()})
            count += 1

            if count % 500 == 0:
                batch_init.commit()
                batch_init = db_client.batch()

        if count > 0:
            batch_init.commit()

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.delete("/delete-patient/{patient_id}", tags=["patient"])
def delete_patient(patient_id: str):
    """ Delete record from the patient's collection. """
    try:
        db_client.collection("patient_simulated_data").document(patient_id).delete()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.get("/patients/search", tags=["search_patients"])
def search_patients(blood_group: str = None, patient_id: str = None, icd_code: str = None):
    """
    Practice Compound Queries (AND/OR logic).
    """
    try:
        collection_ref = db_client.collection("patient_simulated_data")
        # This may require a Composite Index (check terminal for the link

        query = collection_ref.where(
            filter=Or(filters=[
                And(filters=[
                    FieldFilter("blood_group", "==", blood_group),
                    FieldFilter("billed_amount", ">=", 10000.0),
                    FieldFilter("icd_code", "array_contains", icd_code)
                ]),
                FieldFilter("patient_id", "==", patient_id)
            ])
        )
        # query_mul_con = collection_ref.where(
        #     filter=FieldFilter("blood_group", "==", blood_group)).where(
        #     filter=FieldFilter("billed_amount", "==", 10000.0).where(
        #     filter=FieldFilter("icd_code", "array_contains", icd_code)
        #     )
        # )
        results = query.stream()
        return [{res.id: res.to_dict()} for res in results]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
