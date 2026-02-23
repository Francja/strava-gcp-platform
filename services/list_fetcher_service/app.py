import os
import json
import time
import base64
import requests
from fastapi import FastAPI, Request
from google.cloud import firestore, pubsub_v1, storage

app = FastAPI()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")

db = firestore.Client()
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()


def get_valid_token(athlete_id: str) -> str:
    """Get access token from Firestore, refresh if expired."""
    doc = db.collection("strava_tokens").document(athlete_id).get()
    tokens = doc.to_dict()

    # If token expired, refresh it
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

        # Update Firestore with fresh tokens
        db.collection("strava_tokens").document(athlete_id).update({
            "access_token": new_tokens["access_token"],
            "refresh_token": new_tokens["refresh_token"],
            "expires_at": new_tokens["expires_at"],
        })

        return new_tokens["access_token"]

    return tokens["access_token"]


@app.post("/")
async def handle_pubsub(request: Request):
    # Pub/Sub sends messages as base64-encoded JSON wrapped in an envelope
    envelope = await request.json()
    message_data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
    payload = json.loads(message_data)

    athlete_id = payload["athlete_id"]
    access_token = get_valid_token(athlete_id)

    bucket = storage_client.bucket(GCS_BUCKET)
    topic_path = publisher.topic_path(PROJECT_ID, "strava-fetch-detail")

    page = 1
    per_page = 200
    activity_count = 0

    while True:
        response = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": per_page, "page": page},
        )

        activities = response.json()
        if not activities:
            break

        # Save raw JSON page to GCS
        blob = bucket.blob(f"raw/activities/athlete_{athlete_id}/list_page_{page}.json")
        blob.upload_from_string(json.dumps(activities))

        # Publish each activity ID to Pub/Sub
        for activity in activities:
            message = json.dumps({
                "athlete_id": athlete_id,
                "activity_id": activity["id"],
            }).encode("utf-8")
            publisher.publish(topic_path, message)
            activity_count += 1

        page += 1

    return {"status": f"Synced {activity_count} activities for athlete {athlete_id}"}