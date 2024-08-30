import requests
import json
import base64
from requests.exceptions import RequestException

def get_connectors(api_key, environment_id, kafka_cluster_id):
  """Fetches a list of connectors from a Confluent Cloud environment and cluster with details.

  Args:
      api_key (str): Your Confluent Cloud API key.
      environment_id (str): The ID of the Confluent Cloud environment.
      kafka_cluster_id (str): The ID of the Kafka cluster within the environment.

  Returns:
      list: A list of dictionaries containing connector details.
  """

  # Replace with the actual API endpoint URL
  url = f"https://api.confluent.cloud/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors"

  # Set headers with your API key
  auth_string = f"{api_key}:{api_secret}"
  auth_value = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

  headers = {
      "Authorization": f"Basic {auth_value}"
  }

  response = requests.get(url, headers=headers)

  if response.status_code == 200: 
    return json.loads(response.text)
  else:
    print(f"Error getting connector names: {response.status_code} - {response.text}")
    return []

def get_connector_details(api_key, environment_id, kafka_cluster_id, connector_name):
  """Fetches details for a specific connector.

  Args:
      api_key (str): Your Confluent Cloud API key.
      environment_id (str): The ID of the Confluent Cloud environment.
      kafka_cluster_id (str): The ID of the Kafka cluster within the environment.
      connector_name (str): The name of the connector to retrieve details for.

  Returns:
      dict: A dictionary containing the connector's details, or None on errors.
  """

  url = f"https://api.confluent.cloud/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}"

  # Set headers with your API key
  auth_string = f"{api_key}:{api_secret}"
  auth_value = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

  headers = {
      "Authorization": f"Basic {auth_value}"
  }


  response = requests.get(url, headers=headers)

  if response.status_code == 200:
    return json.loads(response.text)
  
  else:
    print(f"Error getting connector details for {connector_name}: {response.status_code} - {response.text}")
    return None

def generate_json(api_key, environment_id, kafka_cluster_id):
  """Fetches connector details and generates a JSON response.

  Args:
      api_key (str): Your Confluent Cloud API key.
      environment_id (str): The ID of the Confluent Cloud environment.
      kafka_cluster_id (str): The ID of the Kafka cluster within the environment.

  Returns:
      dict: A dictionary representing the JSON output.
  """

  assets = []
  sources = []
  lineages = []
  topics = set()

  connector_names = get_connectors(api_key, environment_id, kafka_cluster_id)
  for connector_name in connector_names:
    connector_details = get_connector_details(api_key, environment_id, kafka_cluster_id, connector_name)
    if connector_details:
      # Construct asset based on connector details
      asset_type = "Generic"  # Adjust based on connector type
      if connector_details['config']['connector.class'] == "S3_SINK":
        uri = f"s3://{connector_details['config'].get('s3.bucket.name')}/aws.files"
      elif connector_details['config']['connector.class'] == "DatagenSource":
        uri = "datagen://datagen.random/datagen"
      else:
        uri = f"custom_uri_based_on_{connector_details['config']}"

      asset = {
          "uri": f"{uri}.{connector_name}",
          "name": connector_name,
          "type": asset_type,
          "description": f"Connector: {connector_details['name']}"
      }
      assets.append(asset)

      # Construct source based on connector details
      source = {
          "uri": uri,
          "name": connector_name,
          "description": f"Connector: {connector_details['name']}"
      }
      sources.append(source)


      kafka_topic = connector_details['config'].get('kafka.topic')
      if kafka_topic: topics.add(kafka_topic)

      connector_type = connector_details['type']
      for topic in topics:
        lineage = {
            "from": f"{uri}.{connector_name}" if connector_type == "source" else f"kafka://{kafka_cluster_id}/{topic}",
            "to": f"kafka://{kafka_cluster_id}/{topic}" if connector_type == "source" else f"{uri}.{connector_name}"
        }
        lineages.append(lineage)

  # Add topics as assets
  for topic in topics:
    assets.append({
        "uri": f"kafka://{kafka_cluster_id}/{topic}",
        "name": topic,
        "type": "Pipeline",
        "description": f"Kafka Topic: {topic}"
    })
    sources.append({
        "uri": f"kafka://{kafka_cluster_id}/{topic}",
        "name": topic,
        "description": f"Kafka Topic: {topic}"
    })

  return {
      "workspace": kafka_cluster_id,
      "assets": assets,
      "sources": sources,
      "lineages": lineages
  }

def send_lineage_data(data, access_token, tenant_host):
    """
    Sends lineage data to Sifflet API.

    Args:
      data: The lineage data in JSON format.
      access_token: The Sifflet access token.
      tenant_host: The Sifflet tenant host.
    """
    headers = {
      'accept': 'application/json',
      'authorization': f'Bearer {access_token}',
      'content-type': 'application/json'
    }
    url = f"https://{tenant_host}.siffletdata.com/api/v1/assets/sync?dryRun=false"

    try:
        response = requests.request("POST", url, data=json.dumps(data, indent=4), headers=headers)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
    except RequestException as e:
        print(f"Error: {e}")
        print(response.json())
        # Handle the error, e.g., retry the request, log the error, etc.
    else:
        # Successful response
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        # Process the response data as needed


if __name__ == "__main__":
  
  # Replace with your actual API key, environment ID, and Kafka cluster ID
  api_key = '####'
  api_secret = '####'
  kafka_cluster_id = '####'  # The ID of your Kafka cluster
  environment_id = '####'

  sifflet_token="####"
  sifflet_tenant="####"  

  json_output = generate_json(api_key, environment_id, kafka_cluster_id)
  
  send_lineage_data(json_output, sifflet_token, sifflet_tenant)
  

