import requests
import json
from requests.exceptions import RequestException

def get_airbyte_token(airbyte_url, client_id, client_secret):
    """
    Obtains an Airbyte API token using provided credentials.

    Args:
        airbyte_url (str): The base URL of your Airbyte instance.
        client_id (str): The client ID for authentication.
        client_secret (str): The client secret for authentication.

    Returns:
        str: The obtained API token.
    """

    token_url = f"{airbyte_url}/v1/applications/token"
    headers = {"Content-Type": "application/json"}
    data = {"client_id": client_id, "client_secret": client_secret}

    try:
        response = requests.post(token_url, headers=headers, json=data)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
    except RequestException as e:
        print(f"Error: {e}")
        # Handle the error, e.g., retry the request, log the error, etc.
    else:
        # Successful response
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        # Process the response data as needed

    return response.json()["access_token"]

def get_workspace_name(workspace_id, token):
  """
  Retrieves the name of a workspace from the Airbyte API.

  Args:
      workspace_id (str): The ID of the workspace to retrieve information for.
      token (str): The Airbyte API token for authentication.

  Returns:
      str: The name of the workspace or None if an error occurs.
  """

  # Construct the API URL
  url = f"https://api.airbyte.com/v1/workspaces/{workspace_id}"

  # Set headers with authorization token
  headers = {"Authorization": f"Bearer {token}"}

  try:
    # Send a GET request
    response = requests.get(url, headers=headers)

    # Check for successful response
    if response.status_code == 200:
      # Parse JSON response
      data = response.json()
      return data.get("name")  # Get the "name" field
    else:
      print(f"Error retrieving workspace: {response.status_code}")
      return None
  except RequestException as e:
    print(f"An error occurred: {e}")
    return None

def get_source_details(airbyte_url, token, source_id):
    """
    Fetches name and configuration of a specific source using its ID.

    Args:
        airbyte_url (str): The base URL of your Airbyte instance.
        token (str): The Airbyte API token.
        source_id (str): The ID of the source to retrieve details for.

    Returns:
        dict: A dictionary containing source name, configuration, and potential URI.
    """

    source_url = f"{airbyte_url}/v1/sources/{source_id}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(source_url, headers=headers)
    response.raise_for_status()

    source_data = response.json()
    source_config = source_data.get("configuration", {})
    connector_type = source_data.get("sourceType")
    connector_name = source_data.get("name")

    return {"config": source_config, "sourceType": connector_type, "name": connector_name}

def get_destination_details(airbyte_url, token, destination_id):
    """
    Fetches name and configuration of a specific destination using its ID.

    Args:
        airbyte_url (str): The base URL of your Airbyte instance.
        token (str): The Airbyte API token.
        destination_id (str): The ID of the destination to retrieve details for.

    Returns:
        dict: A dictionary containing destination name, configuration, and potential URI.
    """

    destination_url = f"{airbyte_url}/v1/destinations/{destination_id}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(destination_url, headers=headers)
    response.raise_for_status()

    destination_data = response.json()
    destination_config = destination_data.get("configuration", {})
    connector_type = destination_data.get("destinationType")
    connector_name = destination_data.get("name")

    return {"config": destination_config, "sourceType": connector_type, "name": connector_name}


def get_connections_and_details(airbyte_url, token):
    """
    Fetches all connections from Airbyte and retrieves detailed source and destination information.

    Args:
        airbyte_url (str): The base URL of your Airbyte instance.
        token (str): The Airbyte API token.

    Returns:
        list: A list of dictionaries, each containing connection details, source details, and destination details.
    """

    connections_url = f"{airbyte_url}/v1/connections"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(connections_url, headers=headers)
    response.raise_for_status()

    connections_data = response.json()["data"]

    connection_details = []
    sources = []
    assets = []
    lineages = []
    for connection in connections_data:
        airbyte_asset = {"uri": f"airbyte://cloud.airbyte.com/{connection.get("workspaceId")}.{connection.get("connectionId")}", "name": connection.get("name"), "type": "Pipeline", "href": f"https://cloud.airbyte.com/workspaces/{connection.get("workspaceId")}/connections/{connection.get("connectionId")}/status"}
        workspace_name = get_workspace_name(connection.get("workspaceId"), token)
        airbyte_source = {"uri": f"airbyte://{connection.get("workspaceId")}", "name": workspace_name, "description": f"Airbyte Workspace for {workspace_name}"}

        source_id = connection.get("sourceId")
        destination_id = connection.get("destinationId")

        configuration_data = connection.get("configurations")
        streams_data = configuration_data.get("streams")


        if source_id:
            source_details = get_source_details(airbyte_url, token, source_id)

        if destination_id:
            destination_details = get_destination_details(airbyte_url, token, destination_id)

#       Override the default schema in the destination if the connection has a namespace format

        if connection.get("namespaceFormat"):
            destination_details["config"]["schema"] = connection.get("namespaceFormat")

        source = parse_source_configuration(source_details["config"], source_details["sourceType"], source_details["name"], source_id)
        destination = parse_source_configuration(destination_details["config"], destination_details["sourceType"], destination_details["name"], source_id)

        sources.append(source)
        sources.append(destination)

        for stream in streams_data:

            source_stream = parse_asset_configuration(source_details["config"], source_details["sourceType"], stream["name"])
            destination_stream = parse_asset_configuration(destination_details["config"], destination_details["sourceType"], stream["name"])
            
            assets.append(source_stream)
            assets.append(destination_stream) 

            lineages.append({"from": source_stream.get("uri"), "to": airbyte_asset.get("uri")})
            lineages.append({"from": airbyte_asset.get("uri"), "to": destination_stream.get("uri")})

        assets.append(airbyte_asset) if airbyte_asset not in assets else assets
        sources.append(airbyte_source) if airbyte_source not in sources else sources


        connection_details= {
            "workspace": connection.get("workspaceId"),
            "sources": sources,
            "lineages": lineages,
            "assets": assets
        }

    return connection_details

def parse_asset_configuration(configuration, connector_type, name):
    """
    Parses configuration details and creates a URI if the connector is BigQuery.

    Args:
        configuration (dict): The configuration object.
        connector_type (str): The type of connector (e.g., "bigquery").
        name (str): The name of the source or destination.

    Returns:
        str: The constructed URI if the connector is BigQuery, otherwise an empty string.
    """

    if connector_type == "bigquery":
        project_id = configuration.get("project_id")
        dataset_id = configuration.get("dataset_id")
        return {"uri": f"bigquery://{project_id.upper()}.{dataset_id.upper()}.{name.upper().replace(" ", "_")}", "name": name, "type": "Dataset"}
    elif connector_type == "github":
        repository = configuration.get("repositories")
        return {"uri": f"github://{repository[0]}.{name}", "name": name.upper().replace(" ", "_"), "type": "Dataset"}
    elif connector_type == "snowflake":
        database = configuration.get("database")
        schema = configuration.get("schema")
        delimiter = ".snowflakecomputing.com"
        parts = configuration.get("host").split(delimiter)
        return {"uri": f"snowflake://{parts[0]}/{database.upper()}.{schema.upper()}.{name.upper().replace(" ", "_")}", "name": name, "type": "Dataset"}
    elif connector_type == "s3":
        return {"uri": f"s3://{configuration.get("bucket")}/aws.{name.lower().replace(" ", "_")}", "name": name, "type": "Generic", "description": f"s3://{configuration.get("bucket")}/{name.lower().replace(" ", "_")}"}
    elif connector_type == "slack":
        return {"uri": f"slack://slack.com/slack.{name.lower().replace(" ", "_")}", "name": name, "type": "Dataset"}   
    else:
        #print(configuration)
        return {"uri": None, "name": None}  # Handle other connector types

def parse_source_configuration(configuration, connector_type, name, source_id):
    """
    Parses configuration details and creates a URI if the connector is BigQuery.

    Args:
        configuration (dict): The configuration object.
        connector_type (str): The type of connector (e.g., "bigquery").

    Returns:
        str: The constructed URI if the connector is BigQuery, otherwise an empty string.
    """
    uri=""
    description=""

    if connector_type == "bigquery":
        project_id = configuration.get("project_id")
        dataset_id = configuration.get("dataset_id")
        uri= f"bigquery://{project_id.upper()}.{dataset_id.upper()}"
    elif connector_type == "github":
        repository = configuration.get("repositories")[0]
        uri= f"github://{repository}"
    elif connector_type == "snowflake":
        database = configuration.get("database")
        schema = configuration.get("schema")
        delimiter = ".snowflakecomputing.com"
        parts = configuration.get("host").split(delimiter)
        uri= f"snowflake://{parts[0].upper()}/{database.upper()}.{schema.upper()}"
    elif connector_type == "s3":
        uri = f"s3://{configuration.get("bucket")}"
    elif connector_type == "slack":
        uri = "slack://open"
    else:
        #print(configuration)
        return {"uri": None, "name": None}  # Handle other connector types

    return {"uri": uri, "name": f"{name} connection in Airbyte - Source ID: {source_id}"}

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
    url = f"https://{tenant_host}.siffletdata.com/api/v1/assets/sync?dryRun=true"

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



# Example usage:
airbyte_url = "https://api.airbyte.com"
airbyte_client_id = "####"
airbyte_client_secret = "####"

sifflet_token="####"
sifflet_tenant="####"
token = get_airbyte_token(airbyte_url, airbyte_client_id, airbyte_client_secret)
connection_data = get_connections_and_details(airbyte_url, token)

send_lineage_data(connection_data, sifflet_token, sifflet_tenant)

