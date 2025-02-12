from dotenv import load_dotenv
load_dotenv()

import base64
import json
import os
import requests
import google.generativeai as genai
from google.cloud import bigquery

# --- Configuration ---
TURN_API_KEY = os.environ.get("TURN_API_KEY")
TURN_API_ENDPOINT_BASE = os.environ.get("TURN_API_ENDPOINT_BASE")

if TURN_API_KEY is None:
    raise ValueError("The TURN_API_KEY environment variable is not set.")
if TURN_API_ENDPOINT_BASE is None:
    raise ValueError("The TURN_API_ENDPOINT_BASE environment variable is not set.")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if GEMINI_API_KEY is None:
    raise ValueError(
        "The GEMINI_API_KEY environment variable is not set. "
        "Please set it in the Cloud Run service configuration."
    )
genai.configure(api_key=GEMINI_API_KEY)

PROJECT_ID = os.environ.get("BIGQUERY_PROJECT")
DATASET_ID = os.environ.get("BIGQUERY_DATASET")
TABLE_ID = os.environ.get("BIGQUERY_TABLE")

if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
    raise ValueError("BigQuery project, dataset, or table environment variables are not set.")


# BigQuery Client (using service account credentials from GOOGLE_APPLICATION_CREDENTIALS)
client = bigquery.Client()

# --- Helper Functions ---

# Pull the number's entire message history from Turn data streamed to BigQuery
def fetch_message_history(phone_number):
    """Fetches the message history for a given user ID from BigQuery."""
    message_history_query = f"""
        SELECT
            COALESCE(
                content,
                rendered_content,
                template_button_text
            ) AS message,
            inserted_at
        FROM (
            SELECT
                messages.content,
                messages.direction,
                messages.rendered_content,
                messages.template_button_text,
                messages.inserted_at,
                ROW_NUMBER() OVER (
                    PARTITION BY messages.rendered_content, messages.inserted_at
                    ORDER BY TIMESTAMP(messages.inserted_at)
                ) AS rn
            FROM
                `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS messages  # Correct table name
            WHERE
                (messages.from_addr = '{phone_number}' OR messages.addressees LIKE '%{phone_number}%')
        )
        WHERE
            rn = 1
            AND direction = 'inbound'
            AND COALESCE(
                content,
                rendered_content,
                template_button_text
            ) IS NOT NULL
        ORDER BY
            TIMESTAMP(inserted_at)
        """
    try:
        message_history_job = client.query(message_history_query)
        messages = [msg.message for msg in message_history_job]
        return messages
    except Exception as e:
        print(f"Error fetching message history for {user_id}: {e}")
        return []  # Return an empty list on error

# Generate a short business profile useful as context for future responses
def generate_business_profile_with_gemini(messages):
    """Generates a business profile using the Gemini API."""
    prompt = f"These messages were sent by a microenterprise owner. Write a max 100 word business profile. This profile will not be read by a human, it will be given as context to an LLM that will answering future questions from owner, so be efficient and optimized. Include important context to ensure future advice is relevant and useful, such as key business characteristics like sector, products, etc, as well as the owner's key problems and priorities. Do this in peruvian spanish: {messages}"
    try:
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error generating profile with Gemini: {e}")
        return ""  # Return an empty string on error

# Update business profile stored as a Turn user profile variable
def push_profile_to_journeys(phone_number, business_summary):
    """Pushes the generated business profile to the Turn API."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TURN_API_KEY}",
        "Accept": "application/vnd.v1+json"
    }
    
    # Remove the leading '+' if it exists
    cleaned_phone_number = phone_number.lstrip("+")
    
    # Construct the endpoint dynamically
    turn_api_endpoint = f"{TURN_API_ENDPOINT_BASE}{cleaned_phone_number}/profile"
    payload = {
        "business_profile": business_summary
    }

    try:
        response = requests.patch(
            turn_api_endpoint, headers=headers, json=payload
        )
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        print(f"Successfully pushed profile for {phone_number} to Turn")
    except requests.exceptions.RequestException as e:
        print(f"Error pushing profile to Journeys: {e}")


# --- Main Cloud Function ---

def generate_and_push_profile(event, context):
    """Background Cloud Function to be triggered by Pub/Sub."""

    # --- CORS Handling (for HTTP triggers, important for later) ---
    if event.get("httpMethod") == "OPTIONS": #Simplified CORS
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type, x-api-key',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    # --- Pub/Sub Message Handling ---
    try:
        # FOR LOCAL TESTING:  If called directly with a phone number, use that.
        if isinstance(event, str):  # Check if event is a string (local testing)
            phone_number = event
            print(f"Running in LOCAL TEST MODE with phone number: {phone_number}")
        else: # It's a pub/sub event
            pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
            message_data = json.loads(pubsub_message)
            phone_number = message_data["phone_number"]
            print(f"Received message for phone number: {phone_number}")

    except Exception as e:
        print(f"Error decoding Pub/Sub message: {e}")
        return (f"Error: {e}", 500, headers)  # Return error for Pub/Sub

    # --- Main Logic ---

    # 1. Fetch Message History
    messages = fetch_message_history(phone_number)
    if not messages:
        print(f"No messages found for {phone_number}")
        return (f"No messages found for {phone_number}", 200, headers) # Return 200 even with no messages.

    # 2. Generate Business Profile
    business_summary = generate_business_profile_with_gemini(messages)
    if not business_summary:
        print(f"Failed to generate profile for {phone_number}")
        return (f"Failed to generate profile for {phone_number}", 500, headers)

    # 3. Push Profile to Turn
    push_profile_to_journeys(phone_number, business_summary)
    return ("Profile generated and pushed successfully", 200, headers)

# --- Local Testing (Entry Point) ---
if __name__ == "__main__":
    test_phone_number = "+123456789"  # Replace with a REAL phone number for local testing
    # Simulate the Pub/Sub event structure
    test_event = {
        "data": base64.b64encode(json.dumps({"phone_number": test_phone_number}).encode("utf-8"))
    }
    generate_and_push_profile(test_event, None)  # Pass the dictionary