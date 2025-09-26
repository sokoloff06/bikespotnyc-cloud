# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn, firestore_fn, scheduler_fn, options
# from firebase_functions.options import set_global_options
from firebase_admin import initialize_app, firestore
import google.cloud.firestore
import requests
import json
import hashlib
from typing import Any, Dict, List

# For cost control, you can set the maximum number of containers that can be
# running at the same time. This helps mitigate the impact of unexpected
# traffic spikes by instead downgrading performance. This limit is a per-function
# limit. You can override the limit for each function using the max_instances
# parameter in the decorator, e.g. @https_fn.on_request(max_instances=5).
# set_global_options(max_instances=10)

app = initialize_app()
#
#
# @https_fn.on_request()
# def on_request_example(req: https_fn.Request) -> https_fn.Response:
#     return https_fn.Response("Hello world!")

@https_fn.on_request()
def addmessage(req: https_fn.Request) -> https_fn.Response:
    """Take the text parameter passed to this HTTP endpoint and insert it into
    a new document in the messages collection."""
    # Grab the text parameter.
    original = req.args.get("text")
    if original is None:
        return https_fn.Response("No text parameter provided", status=400)

    firestore_client: google.cloud.firestore.Client = firestore.client()

    # Push the new message into Cloud Firestore using the Firebase Admin SDK.
    _, doc_ref = firestore_client.collection("messages").add({"original": original})

    # Send back a message that we've successfully written the message
    return https_fn.Response(f"Message with ID {doc_ref.id} added.")

@firestore_fn.on_document_created(document="messages/{pushId}")
def makeuppercase(event: firestore_fn.Event[firestore_fn.DocumentSnapshot | None]) -> None:
    """Listens for new documents to be added to /messages. If the document has
    an "original" field, creates an "uppercase" field containg the contents of
    "original" in upper case."""

    # Get the value of "original" if it exists.
    if event.data is None:
        return
    try:
        original = event.data.get("original")
    except KeyError:
        # No "original" field, so do nothing.
        return

    # Set the "uppercase" field.
    print(f"Uppercasing {event.params['pushId']}: {original}")
    upper = original.upper()
    event.data.reference.update({"uppercase": upper})

@scheduler_fn.on_schedule(
    schedule="every day 09:00",
    timeout_sec=540
)
def fetch_nyc_open_data_daily(event: scheduler_fn.ScheduledEvent) -> None:
# @https_fn.on_request()
# def fetch_bike_parking_data(req: https_fn.Request) -> https_fn.Response:
    """
    Fetch data from the NYC Open Data endpoint daily and write/upsert it into
    the Firestore collection `city_bike_data`.

    - Uses the provided app token and paginates through the dataset in batches.
    - Uses a deterministic document id when possible (objectid/id fields),
      otherwise falls back to an MD5 hash of the record JSON.
    - Commits writes in batches of 500 to stay within Firestore limits.
    """

    base_url = "https://data.cityofnewyork.us/resource/592z-n7dk.json"
    app_token = "hu9BzKjWNPK6cyPCojkogHuJc"
    limit = 500
    offset = 0
    total_written = 0

    firestore_client: google.cloud.firestore.Client = firestore.client()
    collection = firestore_client.collection("city_bike_data")

    while True:
        params = {
            "$$app_token": app_token,
            "$limit": limit,
            "$offset": offset,
        }
        print(f"Fetching {limit} records from offset {offset}...")

        try:
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            print(f"Error fetching data at offset {offset}: {e}")
            break  # Stop processing on fetch error

        if not isinstance(payload, list) or not payload:
            print("No more records found or unexpected payload. Finishing.")
            break

        batch = firestore_client.batch()
        batch_count = 0

        for rec in payload:
            if not isinstance(rec, dict):
                continue

            # Try to find a natural primary key in common fields, otherwise hash the record
            doc_id = None
            for candidate in ("site_id", "objectid", "object_id", "id", "uniqueid", "the_geom_id"):
                if candidate in rec:
                    doc_id = str(rec[candidate])
                    break
            if doc_id is None:
                # deterministic fallback
                doc_id = hashlib.md5(json.dumps(rec, sort_keys=True).encode()).hexdigest()

            doc_ref = collection.document(doc_id)
            batch.set(doc_ref, rec)
            batch_count += 1

        if batch_count > 0:
            try:
                batch.commit()
                total_written += batch_count
                print(f"Successfully wrote {batch_count} documents.")
            except Exception as e:
                print(f"Error committing batch for offset {offset}: {e}")
                # Depending on requirements, you might want to break here or retry

        offset += limit

    print(f"Finished writing {total_written} documents to 'city_bike_data'.")
    # return https_fn.Response(f"Bikes data is updated.")