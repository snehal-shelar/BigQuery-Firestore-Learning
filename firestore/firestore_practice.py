from google.cloud.firestore_v1.base_aggregation import CountAggregation, SumAggregation, AvgAggregation
from fastapi import FastAPI, HTTPException
from google.cloud import firestore
from db_config import db_client

app = FastAPI()

@app.get("/patients/stats", tags=["aggregation"])
def get_patient_stats():
    """
    Implements advanced server-side calculations.
    Version 2.23.0 allows multiple aggregations in one network request.
    """
    try:
        collection_ref = db_client.collection("patient_simulated_data")

        # Define the aggregation pipeline
        query = collection_ref.aggregate([
            CountAggregation(alias="total_count"),
            SumAggregation("age", alias="age_sum"),
            AvgAggregation("age", alias="age_avg")
        ])

        # .get() executes the server-side calculation
        results = query.get()

        # Access results using the aliases we provided
        stats = results[0]

        return {
            "total_patients": stats.get("total_count"),
            "average_age": round(stats.get("age_avg"), 2) if stats.get("age_avg") else 0,
            "sum_of_ages": stats.get("age_sum")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Aggregation Error: {str(e)}")
