import os
import json
import time
import base64
import requests
from fastapi import FastAPI, Request
from google.cloud import firestore, storage, bigquery

app = FastAPI()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE", "activities")

db = firestore.Client()
storage_client = storage.Client()
bq_client = bigquery.Client()


def get_valid_token(athlete_id: str) -> str:
    """Get access token from Firestore, refresh if expired."""
    doc = db.collection("strava_tokens").document(athlete_id).get()
    tokens = doc.to_dict()

    if tokens["expires_at"] < time.time():
        response = requests.post(
            "https://www.strava.com/oauth/token",
            data={
                "client_id": os.getenv("STRAVA_CLIENT_ID"),
                "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
            },
        )
        new_tokens = response.json()
        db.collection("strava_tokens").document(athlete_id).update({
            "access_token": new_tokens["access_token"],
            "refresh_token": new_tokens["refresh_token"],
            "expires_at": new_tokens["expires_at"],
        })
        return new_tokens["access_token"]

    return tokens["access_token"]


@app.post("/")
async def handle_pubsub(request: Request):
    envelope = await request.json()
    message_data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
    payload = json.loads(message_data)

    athlete_id = payload["athlete_id"]
    activity_id = payload["activity_id"]

    access_token = get_valid_token(athlete_id)

    # Fetch activity detail from Strava
    response = requests.get(
        f"https://www.strava.com/api/v3/activities/{activity_id}",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    # Handle rate limiting — Pub/Sub will retry after backoff
    if response.status_code == 429:
        raise Exception(f"Rate limited by Strava, will retry activity {activity_id}")

    if not response.ok:
        raise Exception(f"Strava API error {response.status_code} for activity {activity_id}")

    activity = response.json()

    # Save raw JSON to GCS
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"raw/activity_details/athlete_{athlete_id}/{activity_id}.json")
    blob.upload_from_string(json.dumps(activity))

    # # Load to BigQuery
    # table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    # errors = bq_client.insert_rows_json(table_ref, [activity])
    # if errors:
    #     raise Exception(f"BigQuery insert failed: {errors}")

    # Respect Strava rate limit — 100 requests per 15 min
    time.sleep(1)

    return {"status": f"Activity {activity_id} processed for athlete {athlete_id}"}