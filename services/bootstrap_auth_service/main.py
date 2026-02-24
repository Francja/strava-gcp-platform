import os
import json
import requests
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from google.cloud import firestore, pubsub_v1
import logging
logger = logging.getLogger(__name__)

app = FastAPI()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

db = firestore.Client()
publisher = pubsub_v1.PublisherClient()


@app.get("/")
def login():
    strava_url = (
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&approval_prompt=force"
        f"&scope=activity:read_all"
    )
    return RedirectResponse(strava_url)


@app.get("/callback")
def callback(code: str):
    # 1. Exchange code for tokens
    token_response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
        },
    )

    if not token_response.ok:
        return {"error": "Failed to exchange token with Strava"}

    tokens = token_response.json()

    athlete_id = str(tokens["athlete"]["id"])
    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]
    expires_at = tokens["expires_at"]

    # 2. Store tokens in Firestore
    db.collection("strava_tokens").document(athlete_id).set({
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at,
    })

    # Publish do Pub/Sub
    try:
        topic_path = publisher.topic_path(PROJECT_ID, "strava-start-sync")
        message = json.dumps({"athlete_id": athlete_id}).encode("utf-8")
        future = publisher.publish(topic_path, message)
        message_id = future.result()  # waiting for confirmation
        logger.info(f"Published message {message_id} for athlete {athlete_id}")
    except Exception as e:
        logger.error(f"Failed to publish: {e}")
        return {"error": f"Publish failed: {e}"}

    return {"status": f"Authenticated! Sync started for athlete {athlete_id}"}


    return {"status": f"Authenticated! Sync started for athlete {athlete_id}"}
