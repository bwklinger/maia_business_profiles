import base64
import json
import os
import requests
import google.generativeai as genai
from google.cloud import bigquery

# --- Configuration ---
JOURNEYS_API_KEY = "YOUR_JOURNEYS_API_KEY"
JOURNEYS_API_ENDPOINT = "YOUR_JOURNEYS_API_ENDPOINT"

# Gemini API Key (from environment variable)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_API_KEY is None:
    raise ValueError(
        "The GEMINI_API_KEY environment variable is not set. "
        "Please set it in the Cloud Run service configuration."
    )
genai.configure(api_key=GEMINI_API_KEY)

# BigQuery Client (using service account credentials from GOOGLE_APPLICATION_CREDENTIALS)
client = bigquery.Client()

# --- Helper Functions ---

def fetch_message_history(user_id):
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
                `maia-mype-asesor-ia.51907354231.messages` AS messages  # Correct table name
            WHERE
                (messages.from_addr = '{user_id}' OR messages.addressees LIKE '%{user_id}%')
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


def generate_business_profile_with_gemini(messages):
    """Generates a business profile using the Gemini API."""
    prompt = f"These messages were sent by a peruvian microenterprise. Write a max 100 word business profile. THis profile will not be read by a human, it will be given as context to an LLM that will answering future questions from owner, so be efficient and optimized. Include important context to ensure future advice is relevant and useful, such as key business characteristics like sector, products, etc, as well as the owner's key problems and priorities. Do this in peruvian spanish: {messages}"
    try:
        model = genai.GenerativeModel("gemini-pro")  # Or your preferred model
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error generating profile with Gemini: {e}")
        return ""  # Return an empty string on error


def push_profile_to_journeys(phone_number, business_summary):
    """Pushes the generated business profile to the Journeys API."""
    headers = {
        "Content-Type": "application/json",
        "x-api-key": JOURNEYS_API_KEY,  # Use the correct header for Journeys
    }
    payload = {
        "phone_number": phone_number,  # Consistent field name
        "business_profile": business_summary,
    }

    try:
        response = requests.post(
            JOURNEYS_API_ENDPOINT, headers=headers, json=payload
        )
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        print(f"Successfully pushed profile for {phone_number} to Journeys")
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

    # 3. Push Profile to Journeys
    push_profile_to_journeys(phone_number, business_summary)
    return ("Profile generated and pushed successfully", 200, headers)