import requests
import string
import datetime
import time
import random

def get_formatted_data(sifflet_tenant, sifflet_token, alation_tenant, alation_secret_token):
  """Retrieves formatted data based on rule information and lastRunStatus.

  Args:
    sifflet_tenant: The sifflet tenant name.
    sifflet_token: The bearer token for authorization.

  Returns:
    A dictionary containing "fields" and "values" lists in the desired format,
    or None on error.
  """

  rules_url = f"https://{sifflet_tenant}.siffletdata.com/api/v1/rules?page=0&itemsPerPage=400"
  header_url_template = f"https://{sifflet_tenant}.siffletdata.com/api/ui/v1/assets/:urn/header"
  details_url_template = f"https://{sifflet_tenant}.siffletdata.com/api/v1/rules/:rule_id/details"
  monitor_url_template = f"https://{sifflet_tenant}.siffletdata.com/monitors/rule/:rule_id/overview"
  runs_url_template = f"https://{sifflet_tenant}.siffletdata.com/api/v1/rules/:rule_id/runs?page=0&itemsPerPage=1&sort=endDate%2CDESC"
  alation_url = f"https://{alation_tenant}.alationcloud.com/integration/v1/data_quality/"
  alation_datasources_url = f"https://{alation_tenant}.alationcloud.com/integration/v2/datasource/"
  alation_schemas_url_template = f"https://{alation_tenant}.alationcloud.com/integration/v2/datasource/:datasource_id/available_schemas/"
  alation_refresh_token_url = f"https://{alation_tenant}.alationcloud.com/integration/v1/createAPIAccessToken/"

  alation_token_payload = {
    "user_id": str(alation_user_id),
    "refresh_token": alation_secret_token
  }

  alation_token_headers = {
    "accept": "application/json",
    "content-type": "application/json"
  }

  try:
   # Get refreshed Alation API Token
    token_response = requests.post(alation_refresh_token_url, json=alation_token_payload, headers=alation_token_headers)
    token_response.raise_for_status()
    token_data = token_response.json()
    alation_token = token_data.get("api_access_token")

  except requests.exceptions.RequestException as e:
    print(f"Error retrieving Alation Token: {e}")
    return None

  sifflet_headers = {
      "Authorization": f"Bearer {sifflet_token}"
  }

  alation_headers = {
      "TOKEN": alation_token
  }

  try:
   # Get Alation datasources
    datasources_response = requests.get(alation_datasources_url, headers=alation_headers)
    datasources_response.raise_for_status()
    datasources_data = datasources_response.json()
    print(datasources_data)
    # Create a dictionary of datasource ID to schemas
    datasource_schemas = {}
    for datasource in datasources_data:
      datasource_id = datasource['id']
      schemas_url = alation_schemas_url_template.replace(":datasource_id", str(datasource_id))
      schemas_response = requests.get(schemas_url, headers=alation_headers)
      schemas_data = schemas_response.json()
      datasource_schemas[datasource_id] = schemas_data['schemas']

    response = requests.get(rules_url, headers=sifflet_headers)
    response.raise_for_status()

    data = response.json()
    formatted_data = {"fields": [], "values": []}

    for rule in data["searchRules"]["data"]:
            # Check for lastRunStatus
      if "lastRunStatus" not in rule:
        continue  # Skip rules without lastRunStatus

      # Extract relevant information
      rule_id = rule["id"]
      rule_name = rule["name"]
      rule_description = rule["description"]
      criticality = rule.get("criticality", 0)  # Default to 0 if criticality is missing

      datasets = rule["datasets"]
      if datasets:
        dataset = datasets[0]
        urn = dataset["urn"]
        header_url = header_url_template.replace(":urn", urn)

        header_response = requests.get(header_url, headers=sifflet_headers)
        header_response.raise_for_status()

        header_data = header_response.json()
        uri = header_data.get("uri")

        # Get rule details
        details_url = details_url_template.replace(":rule_id", rule_id)
        details_response = requests.get(details_url, headers=sifflet_headers)
        details_response.raise_for_status()

        details_data = details_response.json()
        rule_param = details_data.get("ruleParams")

        # Check for datasetFieldName and update object_type
        dataset_field_name = rule_param.get("datasetFieldName")
        object_type = "TABLE"
        if dataset_field_name:
          uri += f".{dataset_field_name}"
          object_type = "ATTRIBUTE"          

        for datasource_id, schemas in datasource_schemas.items():
          if any(schema in uri for schema in schemas):
            uri_href = uri.split('/')[-1]
            uri = f"{datasource_id}.{uri_href}"
            print(uri)
            break
        
        # Convert timestamp to ISO 8601 format
        last_updated = datetime.datetime.fromtimestamp(rule["lastRunStatus"]["timestamp"] / 1000).isoformat()

        # Get latest run result
        runs_url = runs_url_template.replace(":rule_id", rule_id)
        runs_response = requests.get(runs_url, headers=sifflet_headers)
        runs_response.raise_for_status()

        runs_data = runs_response.json()
        latest_run = runs_data.get("data", [])[0]  # Get first run (assuming latest)
        result = latest_run.get("result")    

        # Build monitor URL
        monitor_url = monitor_url_template.replace(":rule_id", rule_id)

        # Determine status based on lastRunStatus and criticality
        last_run_status = rule["lastRunStatus"]["status"]
        status = "GOOD" if last_run_status == "SUCCESS" else "ALERT"
        if last_run_status != "SUCCESS" and criticality == 3:
          status = "WARNING"          

        # Generate random name and description
        random_name = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
        random_description = "Description for " + random_name

        # Add data to fields and values
        formatted_data["fields"].append({
            "field_key":  f"{rule_id}",
            "name": rule_name,
            "description": rule_description,
            "type": "STRING"
        })
        formatted_data["values"].append({
            "field_key": f"{rule_id}",
            "object_key": uri,  # Assuming uri represents the attribute
            "object_type": object_type,
            "status": status,
            "value": result,
            "url": monitor_url,
            "last_updated": last_updated
        })

 # Post formatted data to Alation
    alation_response = requests.post(alation_url, headers=alation_headers, json=formatted_data)
    alation_response.raise_for_status()
 
    # Extract job status href from response
    job_status_href = alation_response.json().get('href')

    # Construct absolute job status URL
    if job_status_href:
      job_status_url = f"https://{alation_tenant}.alationcloud.com{job_status_href}"
      job_status_response = requests.get(job_status_url, headers=alation_headers)
      job_status_response.raise_for_status()

  except requests.exceptions.RequestException as e:
    print(f"Error retrieving data or posting to Alation: {e}")
    return None

# Example usage:
sifflet_tenant = "####"
sifflet_token = "####"

alation_tenant = "####"
alation_secret_token = "####"
alation_user_id = 31 # replace with the userid for the secret token

get_formatted_data(sifflet_tenant, sifflet_token, alation_tenant, alation_secret_token)
