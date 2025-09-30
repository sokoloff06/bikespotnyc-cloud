# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`
import os

from firebase_functions import https_fn, scheduler_fn, options
from firebase_admin import storage
# from firebase_functions.options import set_global_options
from firebase_admin import initialize_app
import requests
import json
import tempfile

# For cost control, you can set the maximum number of containers that can be
# running at the same time. This helps mitigate the impact of unexpected
# traffic spikes by instead downgrading performance. This limit is a per-function
# limit. You can override the limit for each function using the max_instances
# parameter in the decorator, e.g. @https_fn.on_request(max_instances=5).
# set_global_options(max_instances=10)

app = initialize_app()

def _fetch_and_upload_data():
    """
    Core logic to fetch all bike parking data from the NYC Open Data endpoint,
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
        blob = getBlob()
        # Use a temporary file to handle large data, which is more memory-efficient
        # and avoids payload size limits in the local emulator.
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
            json.dump(all_records, tmp)
            tmp_path = tmp.name
        
        print(f"Temporary file created at {tmp_path}. Uploading to Cloud Storage...")

        # Upload from the temporary file
        blob.upload_from_filename(tmp_path, content_type="application/json")

        # The public URL is available if the object is public (via bucket IAM policy).
        print(f"Successfully uploaded data to {blob.public_url}")

    except Exception as e:
        print(f"Error uploading to Cloud Storage: {e}")
    finally:
        # Clean up the temporary file
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.remove(tmp_path)

@scheduler_fn.on_schedule(
    schedule="every day 09:00",
    timeout_sec=540,
    max_instances=1,
    memory=options.MemoryOption.GB_1, # Increased memory for in-memory aggregation
)
def fetch_nyc_open_data_daily(event: scheduler_fn.ScheduledEvent) -> None:
    """
    A scheduled function that triggers the data fetch and upload process.
    """
    print("Scheduled data fetch triggered.")
    _fetch_and_upload_data()


# This is a helper function for easy debugging in the emulator.
# It's a good practice to wrap it in an emulator check so it doesn't deploy.
if os.environ.get("FUNCTIONS_EMULATOR") == "true":
    # Global flag to ensure the debugger is initialized only once per process.
    _debugger_initialized = False

    @https_fn.on_request()
    def debug_fetch_data(req: https_fn.Request) -> https_fn.Response:
        """HTTP-triggered wrapper to run the data fetch for debugging."""
        import debugpy
        global _debugger_initialized

        # Initialize the debugger only on the first run of this function in the process.
        if not _debugger_initialized:
            _debugger_initialized = True
            print("Debugger active. Waiting for client to attach on port 5678...")
            # Using 0.0.0.0 is robust for containerized environments like the emulator.
            debugpy.listen(("0.0.0.0", 5678))
            debugpy.wait_for_client()
            print("Debugger attached.")
        debugpy.breakpoint
        print("Debug HTTP endpoint triggered. Starting data fetch.")
        _fetch_and_upload_data()
        return https_fn.Response("Debug fetch process initiated. Check logs for details.")


@https_fn.on_request()
def get_bike_data_url(req: https_fn.Request) -> https_fn.Response:
    """
    Returns the public download URL for the aggregated bike data JSON file.
    """
    try:
        blob = getBlob()
        if not blob.exists():
            return https_fn.Response("Bike data file not found. It may not have been generated yet.", status=404)
        
        response_data = {"url": blob.public_url}
        return https_fn.Response(json.dumps(response_data), mimetype="application/json")
    except Exception as e:
        print(f"Error getting blob URL: {e}")
        return https_fn.Response(f"Error getting blob URL: {e}", status=500)

def getBlob():
    storage_bucket_name = 'bikespot-nyc.firebasestorage.app'
    bucket = storage.bucket(storage_bucket_name)
    blob = bucket.blob("files/bike_spots.json")
    return blob

@https_fn.on_request()
def count_bike_spots(req: https_fn.Request) -> https_fn.Response:
    """
    Counts the total number of bike parking spots from the aggregated JSON file
    in Cloud Storage and returns the count.
    """
    try:
        blob = getBlob
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