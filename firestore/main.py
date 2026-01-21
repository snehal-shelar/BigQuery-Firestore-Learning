from fastapi import FastAPI 
from models import CreatePatient
from db_config import db_client

app = FastAPI()


@app.post("/create-patient")
def create_patients(request: CreatePatient):
    """ Create patient's records in patient_simulated_data collection. """

    try:
        db_client.collection('patient_simulated_data').document('patient').set(
        request.__dict__
        )
    except Exception as e:
        raise e 
    
    return True


@app.get("/read-patients")
def read_patients():
    """ Read the patient's collection. """

    patients = db_client.collection('patient_simulated_data')
    records = patients.stream()
    response_data = list()

    for record in records:
        print(f'{record.id} => {record.to_dict()}')
        response_data.append({record.id: record.to_dict()})

    return response_data


@app.patch("/update-patient")
def update_patients(request: CreatePatient):
    """ Update the patient's information. """
 
    try:
        db_client.collection('patient_simulated_data').document('patient').update(request.__dict__)
    except Exception as e:
        raise e 

    return True

# @app.delete("/delete-patient")
# def delete_patient():
#     pass
