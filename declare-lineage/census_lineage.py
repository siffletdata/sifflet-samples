import requests
import json
import sqlparse
from requests.exceptions import RequestException

# Replace with your GetCensus API credentials
API_KEY = "secret-token:####"
API_BASE_URL = "https://app.getcensus.com/api/v1"
WORKSPACE_ID = "#####"

sifflet_token="####"
sifflet_tenant="####"

def get_census_data(endpoint, params={}):
  headers = {
    "Authorization": f"Bearer {API_KEY}"
  }
  response = requests.get(f"{API_BASE_URL}/{endpoint}", headers=headers, params=params)
  response.raise_for_status()  # Raise an exception for error HTTP statuses
  return response.json()

def extract_table_names(parsed_query):
  table_names = []
  for token in parsed_query.tokens:
    if token.ttype is sqlparse.tokens.Token.Keyword and token.value.upper() == 'FROM':
      next_token = token.next
      while next_token and next_token.ttype != sqlparse.tokens.Token.Keyword:
        if next_token.ttype is sqlparse.tokens.Token.Identifier:
          table_names.append(next_token.value)
        next_token = next_token.next
  return table_names

def generate_asset_uri(object_data, source_data, attributes):

  print(object_data)
  print(attributes.get('connection_id'))
  connection_info = get_connection_data(source_data, attributes.get('connection_id'))

  try:
    if object_data['type'] == 'table':
        if connection_info.get('type') == 'snowflake':
            return f"{connection_info.get('type')}://{connection_info.get('connection_details').get('account')}/{object_data['table_catalog']}.{object_data['table_schema']}.{object_data['table_name']}"
        else:
            return f"{connection_info.get('type')}://{object_data['table_catalog']}.{object_data['table_schema']}.{object_data['table_name']}" 
    else:
        return object_data['type']
  except:
    if connection_info.get('type') == 'slack_v2':
        return f"slack://slack.com/slack.{attributes.get('object')}"
    return object_data

def generate_source_uri(object_data, source_data, attributes):

  connection_info = get_connection_data(source_data, attributes.get('connection_id'))

  try:
    if object_data['type'] == 'table':
        if connection_info.get('type') == 'snowflake':
            return f"{connection_info.get('type')}://{connection_info.get('connection_details').get('account')}/{object_data['table_catalog']}.{object_data['table_schema']}"
        else:
            return f"{connection_info.get('type')}://{object_data['table_catalog']}.{object_data['table_schema']}" 
    else:
        return object_data['type']
  except:
    if connection_info.get('type') == 'slack_v2':
        return f"slack://slack.com/slack"
    return object_data

def get_connection_data(arr , id):
    for x in arr["data"]:
        if x["id"] == id:
            return x

def map_to_json_format(census_data):
  json_data = {
    "workspace": "MyFirstDeclaredWorkspace",  # Replace with your workspace name
    "assets": [],
    "sources": [],
    "lineages": []
  }

  # Add workspace as a source
  workspace_uri = f"census://getcensus.com/workspaces.{WORKSPACE_ID}"
  json_data['sources'].append({
    "uri": workspace_uri,
    "name": f"Workspace - {workspace_uri}"
  })

  # Process syncs
  for sync in census_data['syncs']['data']:
    sync_id = sync['id']
    sync_name = sync.get('name') or f"Sync_{sync_id}"  # Use sync ID as default name
    sync_asset_id = f"census://getcensus.com/workspaces.{WORKSPACE_ID}.syncs.{sync_id}"

    json_data['assets'].append({
      "uri": sync_asset_id,
      "name": sync_name,
      "type": "Pipeline",  # Corrected sync type
      # ... other asset fields
    })

#    for data_item in sync['data']:
    source_attributes = sync.get('source_attributes', {})
    destination_attributes = sync.get('destination_attributes', {})

    # Generate source URI
    source_uri = generate_source_uri(source_attributes.get('object', {}), census_data['sources'], source_attributes)
    source_asset_uri = generate_asset_uri(source_attributes.get('object', {}), census_data['sources'], source_attributes)
    source_asset_id = f"{source_asset_uri}"  # Generate a unique ID
    json_data['sources'].append({
    "uri": source_uri,
    "name": f"{source_uri}",  # Replace with appropriate naming
    # ... other source fields
    })

    json_data['assets'].append({
    "uri": source_asset_id,
    "name": f"{source_asset_uri}",  # Replace with appropriate naming
    "type": "Dataset",  # Assuming destination is a type of asset
    # ... other asset fields
    })


    # Generate destination URI
    destination_uri = generate_source_uri(destination_attributes.get('object', {}), census_data['destinations'], destination_attributes)
    destination_asset_uri = generate_asset_uri(destination_attributes.get('object', {}), census_data['destinations'], destination_attributes)
    destination_asset_id = f"{destination_asset_uri}"  # Generate a unique ID

    json_data['sources'].append({
    "uri": destination_uri,
    "name": f"{destination_uri}",  # Replace with appropriate naming
    # ... other source fields
    })


    json_data['assets'].append({
    "uri": destination_asset_id,
    "name": f"{destination_asset_uri}",  # Replace with appropriate naming
    "type": "Dataset",  # Assuming destination is a type of asset
    # ... other asset fields
    })

    # Create lineage
    json_data['lineages'].append({
    "from": source_asset_uri,
    "to": sync_asset_id
    })
    json_data['lineages'].append({
    "from": sync_asset_id,
    "to": destination_asset_id
    })

  return json_data

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

def main():
  # Fetch data from GetCensus
  sources = get_census_data("sources")
  destinations = get_census_data("destinations")
  syncs = get_census_data("syncs")

  # Combine and transform data
  json_data = map_to_json_format({
    "sources": sources,
    "destinations": destinations,
    "syncs": syncs
  })

  send_lineage_data(json_data, sifflet_token, sifflet_tenant)

if __name__ == "__main__":
  main()