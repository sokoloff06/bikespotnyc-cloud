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

def _fetch_and_upload_data(file_format="json"):
    """
    Core logic to fetch all bike parking data from the NYC Open Data endpoint,
    aggregate it into a single file, and upload it to Cloud Storage.
    Supports both 'json' and 'geojson' formats.

    - Uses the provided app token and paginates through the dataset in batches.
    - This approach is optimized for client-side costs, allowing the app to
      download all data in a single request.
    """

    base_url = f"https://data.cityofnewyork.us/resource/592z-n7dk.{file_format}"
    app_token = "hu9BzKjWNPK6cyPCojkogHuJc"
    limit = 5000  # We can use a larger limit as we are not writing to DB in the loop
    offset = 0
    all_records = []
    all_features = []

    # Check if running in the emulator and limit data for local testing
    is_emulator = os.environ.get("FUNCTIONS_EMULATOR") == "true"
    max_pages_in_emulator = 2  # Fetch 2 pages (e.g., 10,000 records) in emulator

    while True:
        params = {
            "$$app_token": app_token,
            "$limit": limit,
            "$offset": offset,
        }
        print(f"Fetching ${file_format} records from offset {offset}...")

        try:
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"Error fetching data at offset {offset}: {e}")
            break  # Stop processing on fetch error

        if file_format == "geojson":
            # For GeoJSON, we aggregate the 'features' array from each paginated response
            # The loop should terminate if the response is not a dictionary or if the 'features' list is empty.
            if not isinstance(records, dict) or "features" not in records or not records["features"]:
                print("No more GeoJSON features found or unexpected payload. Finishing.")
                break
            else:
                all_features.extend(records["features"])
        else:
            # For JSON, the response is a list of records.
            # The loop should terminate if the response is not a list or if the list is empty.
            if not isinstance(records, list) or not records:
                print("No more JSON records found or unexpected payload. Finishing.")
                break
            all_records.extend(records)

        # In emulator mode, stop after a few pages to avoid the payload size limit
        if is_emulator and (offset / limit) >= (max_pages_in_emulator - 1):
            print("Emulator mode: Limiting fetch to a smaller dataset.")
            break

        offset += limit
    if (file_format == "json" and not all_records) or \
       (file_format == "geojson" and not all_features):
        print("No records were fetched. Aborting file upload.")
        return

    if file_format == "geojson":
        # Reconstruct the final FeatureCollection
        final_data = {"type": "FeatureCollection", "features": all_features}
        record_count = len(all_features)
    else:
        final_data = all_records
        record_count = len(all_records)

    print(f"Total records fetched for {file_format}: {record_count}. Preparing to upload to Cloud Storage.")

    try:
        blob = getBlob(f"files/bike_spots.{file_format}")
        content_type = "application/geo+json" if file_format == "geojson" else "application/json"

        # Use a temporary file to handle large data, which is more memory-efficient
        # and avoids payload size limits in the local emulator.
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=f".{file_format}") as tmp:
            json.dump(final_data, tmp)
            tmp_path = tmp.name
        
        print(f"Temporary file created at {tmp_path}. Uploading to Cloud Storage...")

        # Upload from the temporary file
        blob.upload_from_filename(tmp_path, content_type=content_type)

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
    print("Scheduled data fetch triggered for JSON format.")
    _fetch_and_upload_data(file_format="json")

    print("Scheduled data fetch triggered for GeoJSON format.")
    _fetch_and_upload_data(file_format="geojson")


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
        debugpy.breakpoint()
        print("Debug HTTP endpoint triggered. Starting data fetch.")
        _fetch_and_upload_data(file_format="json")
        _fetch_and_upload_data(file_format="geojson")
        return https_fn.Response("Debug fetch process initiated. Check logs for details.")

# Use ?format=json or geojson to make request
@https_fn.on_request()
def get_bike_data_url(req: https_fn.Request) -> https_fn.Response:
    """
    Returns the public download URL for the aggregated bike data JSON file.
    """
    # Determine the requested format, defaulting to 'json' for backward compatibility.
    file_format = req.args.get("format", "json")
    if file_format not in ["json", "geojson"]:
        return https_fn.Response("Invalid format specified. Use 'json' or 'geojson'.", status=400)

    try:
        file_path = f"files/bike_spots.{file_format}"
        blob = getBlob(file_path)
        if not blob.exists():
            return https_fn.Response(f"Bike data file '{file_path}' not found. It may not have been generated yet.", status=404)
        
        response_data = {"url": blob.public_url}
        return https_fn.Response(json.dumps(response_data), mimetype="application/json")
    except Exception as e:
        print(f"Error getting blob URL: {e}")
        return https_fn.Response(f"Error getting blob URL: {e}", status=500)

def getBlob(file_path: str):
    """
    Gets a reference to a blob in Cloud Storage from a given file path.
    """
    storage_bucket_name = 'bikespot-nyc.firebasestorage.app'
    bucket = storage.bucket(storage_bucket_name)
    blob = bucket.blob(file_path)
    return blob

@https_fn.on_request()
def count_bike_spots(req: https_fn.Request) -> https_fn.Response:
    """
    Counts the total number of bike parking spots from the aggregated data file
    in Cloud Storage. Supports both 'json' and 'geojson' formats via a query parameter.
    """
    # Determine the requested format, defaulting to 'json'.
    file_format = req.args.get("format", "json")
    if file_format not in ["json", "geojson"]:
        return https_fn.Response("Invalid format specified. Use 'json' or 'geojson'.", status=400)

    try:
        file_path = f"files/bike_spots.{file_format}"
        blob = getBlob(file_path)
        if not blob.exists():
            return https_fn.Response(f"Bike data file '{file_path}' not found. It may not have been generated yet.", status=404)

        # Download the contents of the blob as a string and parse it as JSON
        data = json.loads(blob.download_as_string())

        spot_count = 0
        if file_format == "geojson":
            # For GeoJSON, the count is the number of features in the FeatureCollection
            if isinstance(data, dict) and "features" in data:
                spot_count = len(data["features"])
        else:
            # For JSON, the data is a list of records, so the count is its length
            if isinstance(data, list):
                spot_count = len(data)

        response_data = {"count": spot_count}
        return https_fn.Response(json.dumps(response_data), mimetype="application/json")
    except Exception as e:
        print(f"Error counting bike spots: {e}")
        return https_fn.Response(f"Error processing bike data: {e}", status=500)