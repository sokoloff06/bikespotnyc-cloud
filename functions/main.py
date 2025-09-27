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
import tempfile
import os

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
    memory=options.MemoryOption.GB_1, # Increased memory for in-memory aggregation
)
def fetch_nyc_open_data_daily(event: scheduler_fn.ScheduledEvent) -> None:
    """
    Fetch all bike parking data from the NYC Open Data endpoint daily,
    aggregate it into a single JSON file, and upload it to Cloud Storage.

    - Uses the provided app token and paginates through the dataset in batches.
    - This approach is optimized for client-side costs, allowing the app to
      download all data in a single request.
    """

    base_url = "https://data.cityofnewyork.us/resource/592z-n7dk.json"
    app_token = "hu9BzKjWNPK6cyPCojkogHuJc"
    limit = 5000  # We can use a larger limit as we are not writing to DB in the loop
    offset = 0
    all_records = []

    # Check if running in the emulator and limit data for local testing
    is_emulator = os.environ.get("FUNCTIONS_EMULATOR") == "true"
    max_pages_in_emulator = 2  # Fetch 2 pages (e.g., 10,000 records) in emulator

    while True:
        params = {
            "$$app_token": app_token,
            "$limit": limit,
            "$offset": offset,
        }
        print(f"Fetching records from offset {offset}...")

        try:
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"Error fetching data at offset {offset}: {e}")
            break  # Stop processing on fetch error

        if not isinstance(records, list) or not records:
            print("No more records found or unexpected payload. Finishing.")
            break

        all_records.extend(records)

        # In emulator mode, stop after a few pages to avoid the payload size limit
        if is_emulator and (offset / limit) >= (max_pages_in_emulator - 1):
            print("Emulator mode: Limiting fetch to a smaller dataset.")
            break

        offset += limit

    if not all_records:
        print("No records were fetched. Aborting file upload.")
        return

    print(f"Total records fetched: {len(all_records)}. Preparing to upload to Cloud Storage.")

    try:
        # Get the default bucket
        bucket = storage.bucket()
        blob = bucket.blob("city_bike_data/all_spots.json")

        # Use a temporary file to handle large data, which is more memory-efficient
        # and avoids payload size limits in the local emulator.
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
            json.dump(all_records, tmp)
            tmp_path = tmp.name
        
        print(f"Temporary file created at {tmp_path}. Uploading to Cloud Storage...")

        # Upload from the temporary file
        blob.upload_from_filename(tmp_path, content_type="application/json")

        # The local emulator does not support ACLs, so we skip making the blob public.
        # In production, this makes the file publicly readable.
        if not is_emulator:
            blob.make_public()
            print(f"Successfully uploaded data to {blob.public_url}")
        else:
            # In the emulator, the public URL isn't directly available in the same way.
            print(f"Successfully uploaded data to emulator. Object path: {blob.name}")

    except Exception as e:
        print(f"Error uploading to Cloud Storage: {e}")
    finally:
        # Clean up the temporary file
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.remove(tmp_path)


@https_fn.on_request()
def get_bike_data_url(req: https_fn.Request) -> https_fn.Response:
    """
    Returns the public download URL for the aggregated bike data JSON file.
    """
    try:
        bucket = storage.bucket()
        blob = bucket.blob("city_bike_data/all_spots.json")
        if not blob.exists():
            return https_fn.Response("Bike data file not found. It may not have been generated yet.", status=404)
        
        response_data = {"url": blob.public_url}
        return https_fn.Response(json.dumps(response_data), mimetype="application/json")
    except Exception as e:
        print(f"Error getting blob URL: {e}")
        return https_fn.Response(f"Error getting blob URL: {e}", status=500)


@https_fn.on_request()
def count_bike_spots(req: https_fn.Request) -> https_fn.Response:
    """
    Counts the total number of bike parking spots from the aggregated JSON file
    in Cloud Storage and returns the count.
    """
    try:
        # Get the default bucket
        bucket = storage.bucket()
        blob = bucket.blob("city_bike_data/all_spots.json")

        if not blob.exists():
            return https_fn.Response("Bike data file not found. It may not have been generated yet.", status=404)

        # Download the contents of the blob as a string and parse it as JSON
        data = json.loads(blob.download_as_string())

        # The data is a list of records, so the count is the length of the list
        spot_count = len(data)

        response_data = {"count": spot_count}
        return https_fn.Response(json.dumps(response_data), mimetype="application/json")
    except Exception as e:
        print(f"Error counting bike spots: {e}")
        return https_fn.Response(f"Error processing bike data: {e}", status=500)