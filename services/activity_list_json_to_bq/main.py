from fastapi import FastAPI, Request
from google.cloud import storage

app = FastAPI()

# Stałe (bez ENV)
BUCKET = "strava_data"
SRC_PREFIX = "raw_data/lists/"
DST_PREFIX = "raw_data/lists_processed/"

# ********** 1) Eventarc -> Przetwarzanie POJEDYNCZEGO pliku **********
@app.post("/process")
async def process_single(req: Request):
    """
    Odbiera CloudEvent z Eventarc (GCS object finalized).
    Przenosi JEDEN obiekt: copy -> delete.
    """
    event = await req.json()
    object_name = event.get("data", {}).get("name")
    bucket_name = event.get("data", {}).get("bucket", BUCKET)

    if not object_name:
        return {"status": "ignored", "reason": "no object name in event"}

    # Pilnujemy tylko naszego folderu
    if not object_name.startswith(SRC_PREFIX):
        return {"status": "skipped", "object": object_name}

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    src_blob = bucket.blob(object_name)
    suffix = object_name[len(SRC_PREFIX):]
    dst_name = f"{DST_PREFIX}{suffix}"

    bucket.copy_blob(src_blob, bucket, dst_name)
    src_blob.delete()

    return {"status": "moved", "from": object_name, "to": dst_name}
