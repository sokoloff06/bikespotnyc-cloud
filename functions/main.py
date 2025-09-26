# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn, firestore_fn, scheduler_fn, options
from firebase_admin import storage
# from firebase_functions.options import set_global_options
from firebase_admin import initialize_app, firestore
import google.cloud.firestore
import requests
import json
import hashlib

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

def _get_document_id(record: dict) -> str:
    """
    Determines a deterministic document ID from a record.
    Tries common ID fields first, otherwise falls back to a hash of the record.
    """
    # Try to find a natural primary key in common fields
    for candidate in ("site_id", "objectid", "object_id", "id", "uniqueid", "the_geom_id"):
        if candidate in record:
            return str(record[candidate])
    
    # Deterministic fallback if no common ID field is found
    return hashlib.md5(
        json.dumps(record, sort_keys=True).encode()
    ).hexdigest()

@scheduler_fn.on_schedule(
    schedule="every day 09:00",
    timeout_sec=540,
    max_instances=1,
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
    total_skipped = 0
    total_processed = 0

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

        # For efficiency, get all document IDs from the payload first.
        doc_ids_in_payload = [_get_document_id(rec) for rec in payload if isinstance(rec, dict)]
        if not doc_ids_in_payload:
            continue

        # Firestore's 'in' query is limited to 30 values. We must chunk the IDs.
        chunk_size = 30
        id_chunks = [doc_ids_in_payload[i:i + chunk_size] for i in range(0, len(doc_ids_in_payload), chunk_size)]

        existing_docs = {}
        for chunk in id_chunks:
            # Fetch existing documents for the current chunk.
            chunk_docs_ref = collection.where("__name__", "in", chunk).stream()
            for doc in chunk_docs_ref:
                existing_docs[doc.id] = doc.to_dict()

        batch = firestore_client.batch()
        batch_count = 0

        for rec in payload:
            if not isinstance(rec, dict):
                continue
            
            total_processed += 1
            doc_id = _get_document_id(rec)

            # Only write if the document is new or if the data has changed.
            if doc_id not in existing_docs or existing_docs[doc_id] != rec:
                doc_ref = collection.document(doc_id)
                batch.set(doc_ref, rec)
                batch_count += 1
            else:
                total_skipped += 1

        if batch_count > 0:
            try:
                batch.commit()
                total_written += batch_count
                print(f"Successfully wrote {batch_count} documents.")
            except Exception as e:
                print(f"Error committing batch for offset {offset}: {e}")
                # Depending on requirements, you might want to break here or retry

        offset += limit

    print(f"Finished. Processed: {total_processed}, Written: {total_written}, Skipped: {total_skipped}.")
    # return https_fn.Response(f"Bikes data is updated.")


@https_fn.on_request()
def count_bike_data(req: https_fn.Request) -> https_fn.Response:
    """
    Counts and returns the total number of documents in the 'city_bike_data'
    collection using an efficient aggregation query.
    """
    try:
        firestore_client: google.cloud.firestore.Client = firestore.client()
        collection_ref = firestore_client.collection("city_bike_data")

        # Use the .count() aggregation for an efficient query.
        count_query = collection_ref.count()
        result = count_query.get()
        # The result is a list containing one CountAggregationResult object.
        count = result[0][0].value

        response_data = {"collection": "city_bike_data", "count": count}
        return https_fn.Response(json.dumps(response_data), mimetype="application/json")
    except Exception as e:
        print(f"Error counting documents: {e}")
        return https_fn.Response(f"Error counting documents: {e}", status=500)