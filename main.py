from dotenv import load_dotenv
load_dotenv() # Loads environment variables from .env file for local development

import base64
import json
import os
import requests
import google.generativeai as genai
from google.cloud import bigquery

# --- Configuration ---
# Attempt to get environment variables
TURN_API_KEY = os.environ.get("TURN_API_KEY")
TURN_API_ENDPOINT_BASE = os.environ.get("TURN_API_ENDPOINT_BASE") # e.g., "https://whatsapp.turn.io/v1/contacts/"

# Validate that Turn API environment variables are set
if TURN_API_KEY is None:
    raise ValueError("The TURN_API_KEY environment variable is not set.")
if TURN_API_ENDPOINT_BASE is None:
    raise ValueError("The TURN_API_ENDPOINT_BASE environment variable is not set. It should look like 'https://your.turn.endpoint/v1/contacts/'")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if GEMINI_API_KEY is None:
    raise ValueError(
        "The GEMINI_API_KEY environment variable is not set. "
        "Please set it in the Cloud Function's environment variables."
    )
# Configure the Gemini client
try:
    genai.configure(api_key=GEMINI_API_KEY)
except Exception as e:
    raise ValueError(f"Failed to configure Gemini API. Check your GEMINI_API_KEY and API access: {e}")


# BigQuery Configuration
PROJECT_ID = os.environ.get("BIGQUERY_PROJECT")
DATASET_ID = os.environ.get("BIGQUERY_DATASET")
TABLE_ID = os.environ.get("BIGQUERY_TABLE") # This is the table where Turn streams message data

# Validate BigQuery environment variables
if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
    raise ValueError("One or more BigQuery environment variables (BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE) are not set.")

# Initialize BigQuery Client
# For local development, ensure GOOGLE_APPLICATION_CREDENTIALS points to your service account key JSON file.
# In a Cloud Function, it uses the function's runtime service account by default.
try:
    client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    raise RuntimeError(f"Failed to initialize BigQuery client. Ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly for local dev or the Cloud Function service account has BigQuery permissions. Error: {e}")

# --- Helper Functions ---

def fetch_message_history(phone_number: str):
    """
    Fetches the inbound message history for a given phone number from BigQuery.
    It selects the actual message content, handling different possible fields
    and deduplicates messages based on content and timestamp.
    """
    print(f"Fetching message history for: {phone_number}")
    # This query assumes your BigQuery table schema includes:
    # - `content`: text of the message
    # - `rendered_content`: potentially for templates or rich messages
    # - `template_button_text`: text from button clicks
    # - `inserted_at`: timestamp of the message
    # - `direction`: 'inbound' or 'outbound'
    # - `from_addr`: sender's address (for inbound, this is the user)
    # - `addressees`: recipient addresses (for outbound, this contains the user)
    message_history_query = f"""
        SELECT
            COALESCE(
                messages.content,
                messages.rendered_content,
                messages.template_button_text
            ) AS message,
            messages.inserted_at
        FROM (
            SELECT
                messages.content,
                messages.direction,
                messages.rendered_content,
                messages.template_button_text,
                messages.inserted_at,
                -- Deduplication step: If multiple identical messages arrive at nearly the same time, take one.
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(messages.content, messages.rendered_content, messages.template_button_text), messages.inserted_at
                    ORDER BY TIMESTAMP(messages.inserted_at)
                ) AS rn
            FROM
                `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS messages
            WHERE
                -- Check if the phone number is either the sender or in the list of addressees
                (messages.from_addr = '{phone_number}' OR messages.addressees LIKE '%{phone_number}%')
        ) AS messages
        WHERE
            rn = 1  -- Ensure unique messages
            AND direction = 'inbound' -- Only consider messages sent BY the user
            AND COALESCE( -- Ensure the message content is not null
                message, -- Already aliased from the outer select's COALESCE
                rendered_content,
                template_button_text
            ) IS NOT NULL
        ORDER BY
            TIMESTAMP(inserted_at) ASC -- Get messages in chronological order
        LIMIT 50 -- Fetch the last 50 messages to keep the prompt concise
        """
    try:
        print(f"Executing BigQuery query: {message_history_query}")
        message_history_job = client.query(message_history_query)
        # Extract the 'message' content from each row
        messages = [str(row.message) for row in message_history_job if row.message is not None]
        print(f"Found {len(messages)} messages for {phone_number}.")
        return messages
    except Exception as e:
        # Corrected variable name in the error message
        print(f"Error fetching message history for {phone_number}: {e}")
        return [] # Return an empty list on error to allow the function to continue gracefully

def generate_business_profile_with_gemini(messages: list, phone_number: str):
    """
    Generates a concise business profile using the Gemini API based on message history.
    The profile is intended for an LLM, optimized for context, in Peruvian Spanish.
    """
    if not messages:
        print(f"No messages provided to generate profile for {phone_number}. Skipping Gemini.")
        return ""

    # Join messages into a single string
    message_text = "\n".join(messages)

    # Construct the prompt for the Gemini API
    prompt = (
        "Estos mensajes fueron enviados por el propietario de una microempresa. "
        "Escribe un perfil de negocio de máximo 100 palabras. "
        "Este perfil NO será leído por un humano, se entregará como contexto a una LLM que responderá "
        "futuras preguntas del propietario. Por lo tanto, sé eficiente y optimizado. "
        "Incluye contexto importante para asegurar que los futuros consejos sean relevantes y útiles, "
        "tales como características clave del negocio (sector, productos, etc.), "
        "así como los principales problemas y prioridades del propietario. "
        f"Historial de mensajes:\n{message_text}"
    )
    print(f"Generating profile for {phone_number} with Gemini. Prompt length: {len(prompt)}")

    try:
        # Using "gemini-2.0-flash" as requested by the user.
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(prompt)
        # It's good practice to check if response.parts exists and has content
        if response.parts:
            profile_text = response.text # .text is a helper that concatenates text from parts
            print(f"Successfully generated profile for {phone_number}: {profile_text[:100]}...") # Log snippet
            return profile_text
        else:
            print(f"Gemini response for {phone_number} had no parts/text. Full response: {response}")
            return ""
    except Exception as e:
        print(f"Error generating profile with Gemini for {phone_number}: {e}")
        return "" # Return an empty string on error

def push_profile_to_turn(phone_number: str, business_summary: str):
    """
    Pushes the generated business profile to the Turn API for the given phone number.
    It updates a custom field, assumed to be 'business_profile'.
    """
    if not business_summary:
        print(f"No business summary to push for {phone_number}.")
        return

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TURN_API_KEY}",
        "Accept": "application/vnd.v1+json" # Standard Turn API accept header
    }

    # Remove the leading '+' from the phone number if it exists, as Turn API might expect E.164 without '+'
    cleaned_phone_number = phone_number.lstrip("+")

    # Construct the dynamic endpoint for updating a contact's profile
    # Example: https://whatsapp.turn.io/v1/contacts/1234567890/profile
    turn_api_endpoint = f"{TURN_API_ENDPOINT_BASE.rstrip('/')}/{cleaned_phone_number}/profile"

    # The payload should match what Turn API expects for updating profile/custom fields.
    # Assuming 'business_profile' is a custom field you've set up in Turn.
    payload = {
        "business_profile": business_summary # Or {"fields": {"business_profile": business_summary}} if Turn needs it nested
    }
    print(f"Pushing profile for {cleaned_phone_number} to Turn: {turn_api_endpoint}")
    print(f"Payload: {json.dumps(payload)}")

    try:
        response = requests.patch(turn_api_endpoint, headers=headers, json=payload)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        print(f"Successfully pushed profile for {cleaned_phone_number} to Turn. Status: {response.status_code}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error pushing profile to Turn for {cleaned_phone_number}: {http_err} - Response: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error pushing profile to Turn for {cleaned_phone_number}: {req_err}")
    except Exception as e:
        print(f"Generic error pushing profile to Turn for {cleaned_phone_number}: {e}")


# --- Main Cloud Function Entry Point ---

def generate_and_push_profile(event, context):
    """
    Background Cloud Function triggered by Pub/Sub.
    Processes a message containing a phone number, fetches its history,
    generates a business profile, and pushes it to the Turn API.
    """
    # --- CORS Handling (for potential HTTP invocation, not primary for Pub/Sub) ---
    # This is more relevant if you plan to call this function directly via HTTP.
    # For Pub/Sub triggers, this OPTIONS pre-flight handling isn't typically used.
    if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
        headers = {
            'Access-Control-Allow-Origin': '*', # Restrict this in production
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-api-key', # Add any custom headers
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Default headers for actual responses (if HTTP triggered)
    response_headers = {
        'Access-Control-Allow-Origin': '*' # Restrict this in production
    }

    # --- Pub/Sub Message Handling ---
    phone_number = None
    try:
        # Check if 'data' exists in the event, typical for Pub/Sub
        if "data" in event:
            pubsub_message_str = base64.b64decode(event["data"]).decode("utf-8")
            print(f"Decoded Pub/Sub message: {pubsub_message_str}")
            message_data = json.loads(pubsub_message_str)
            phone_number = message_data.get("phone_number")
            if not phone_number:
                print("Error: 'phone_number' not found in Pub/Sub message payload.")
                # For HTTP triggers, you might return an error tuple
                # For Pub/Sub, raising an error or returning non-2xx might cause retries.
                # Depending on retry policy, it might be better to log and return success to avoid infinite retries for bad messages.
                return ("Error: 'phone_number' missing in payload.", 400, response_headers)
            print(f"Received Pub/Sub trigger for phone number: {phone_number}")
        # FOR LOCAL TESTING: If event is a string, treat it as the phone number directly.
        elif isinstance(event, str):
            phone_number = event
            print(f"Running in LOCAL DIRECT CALL MODE with phone number: {phone_number}")
        # FOR LOCAL TESTING (simulating HTTP POST with JSON body):
        elif isinstance(event, dict) and "phone_number" in event: # Assuming direct JSON payload for HTTP test
            phone_number = event["phone_number"]
            print(f"Running in LOCAL HTTP JSON TEST MODE with phone number: {phone_number}")
        else:
            print(f"Error: Unrecognized event structure or missing phone_number. Event: {event}")
            return ("Error: Invalid request format or missing phone_number.", 400, response_headers)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from Pub/Sub message: {e}. Message string: {pubsub_message_str}")
        return (f"Error decoding JSON: {e}", 400, response_headers)
    except Exception as e:
        print(f"Error processing event data: {e}")
        return (f"Error processing event: {e}", 500, response_headers)

    # --- Main Logic ---

    # 1. Fetch Message History from BigQuery
    messages = fetch_message_history(phone_number)
    if not messages:
        # If no messages, there's nothing to process.
        # This isn't necessarily an error, could be a new user.
        print(f"No messages found for {phone_number}. Profile generation will be skipped.")
        # Depending on requirements, you might still want to push an empty/default profile
        # or simply acknowledge successful processing of the event.
        return (f"No messages found for {phone_number}. Profile not generated.", 200, response_headers)

    # 2. Generate Business Profile using Gemini
    business_summary = generate_business_profile_with_gemini(messages, phone_number)
    if not business_summary:
        # If profile generation fails, log it.
        # Decide if this is a critical error that should cause the function to fail (and potentially retry).
        print(f"Failed to generate business profile for {phone_number}. Check Gemini logs/API status.")
        return (f"Failed to generate profile for {phone_number}.", 500, response_headers) # Or 200 if you don't want retries

    # 3. Push Profile to Turn API
    push_profile_to_turn(phone_number, business_summary)

    # If everything completes, return a success message.
    print(f"Profile processed and attempt to push made for {phone_number}.")
    return ("Profile generation and push attempt completed successfully.", 200, response_headers)

# --- Local Testing (Entry Point) ---
if __name__ == "__main__":
    print("Starting local test run...")
    # --- OPTION 1: Simulate a Pub/Sub event ---
    # Replace with a REAL phone number that has data in your BigQuery table for testing
    test_phone_number_pubsub = "+12345678901" # Use E.164 format
    test_event_pubsub = {
        "data": base64.b64encode(json.dumps({"phone_number": test_phone_number_pubsub}).encode("utf-8"))
    }
    print(f"\n--- Testing with simulated Pub/Sub event for {test_phone_number_pubsub} ---")
    generate_and_push_profile(test_event_pubsub, None)

    # --- OPTION 2: Simulate a direct call (like your original test) ---
    # test_phone_number_direct = "+1112223333"
    # print(f"\n--- Testing with direct phone number input: {test_phone_number_direct} ---")
    # generate_and_push_profile(test_phone_number_direct, None)

    # --- OPTION 3: Simulate an HTTP POST request with JSON body ---
    # test_phone_number_http = "+447911123456"
    # test_event_http = {"phone_number": test_phone_number_http} # No 'data' wrapper, just the JSON body
    # print(f"\n--- Testing with simulated HTTP JSON POST for {test_phone_number_http} ---")
    # generate_and_push_profile(test_event_http, None)

    print("\nLocal test run finished.")

