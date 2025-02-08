import base64
import json
import os
import requests
import google.generativeai as genai
from google.cloud import bigquery

# Configure Gemini API
# Assuming you are using a service account, the key should be in a secret
# and the GOOGLE_APPLICATION_CREDENTIALS environment variable should be set
genai.configure(api_key=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))

# Journeys API Details
JOURNEYS_API_KEY = "YOUR_JOURNEYS_API_KEY"  # Replace with your Journeys API key
JOURNEYS_API_ENDPOINT = (
    "YOUR_JOURNEYS_API_ENDPOINT"  # Replace with your Journeys API endpoint
)

# BigQuery Client
client = bigquery.Client()

# Function to generate and push business profiles
def generate_and_push_profile(event, context):
    """Background Cloud Function to be triggered by Pub/Sub."""
    # Decode Pub/Sub message
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    message_data = json.loads(pubsub_message)
    phone_number = message_data["phone_number"]

    print(f"Received message for phone number: {phone_number}")

    # Fetch message history from BigQuery
    messages = fetch_message_history(phone_number)

    # Generate business profile with Gemini
    business_summary = generate_business_profile_with_gemini(messages)

    # Push the business profile to Journeys
    push_profile_to_journeys(phone_number, business_summary)

    print(f"Processed profile for {phone_number}")


# Function to fetch message history from BigQuery
def fetch_message_history(user_id):
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
                `your-project.your-dataset.messages` AS messages
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
    message_history_job = client.query(message_history_query)
    messages = [msg.message for msg in message_history_job]
    return messages


# Function to generate business profile with Gemini
def generate_business_profile_with_gemini(messages):
    prompt = f"These messages were sent by a peruvian microenterprise. Write a susinct business profile that will be provided as context to an LLM answering future questions. Include important context to make future responses most useful, such as business characteristics like sector, products, etc, as well as the owner's key problems and priorities: {messages}"
    model = genai.GenerativeModel("gemini-pro")
    response = model.generate_content(prompt)
    return response.text


# Function to push the business profile to Journeys
def push_profile_to_journeys(phone_number, business_summary):
    headers = {
        "Content-Type": "application/json",
        "x-api-key": JOURNEYS_API_KEY,  # Or appropriate auth headers
    }
    payload = {
        "phone_number": phone_number,
        "business_profile": business_summary,
    }

    try:
        response = requests.post(
            JOURNEYS_API_ENDPOINT, headers=headers, json=payload
        )
        response.raise_for_status()
        print(
            f"Successfully pushed profile for {phone_number} to Journeys"
        )

    except requests.exceptions.RequestException as e:
        print(f"Error pushing profile to Journeys: {e}")