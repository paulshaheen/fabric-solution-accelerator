# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # Fabric Launcher + SEMPy – Sample Installer Notebook
# 
# This notebook is a **starting point** for deploying a Fabric solution from a repo (via **Fabric Launcher**) and then validating / interacting with a semantic model using **SEMPy**.
# 
# **Review the readme before starting**
# 
# **What you’ll do:**
# 1. Install/Import libraries
# 2. Set your workspace + repo parameters
# 3. (Optional) Deploy items using Fabric Launcher
# 4. Discover semantic models and run a simple DAX query
# 
# > Notes: Library names/APIs can vary by version/preview. Adjust imports based on the package versions available in your Fabric tenant.


# MARKDOWN ********************

# ## Install Libs


# CELL ********************

%pip install semantic-link -q
#%pip install -U sempy -q #this is already installed by default on fabric compute
%pip install -U fabric-launcher -q

notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import sys, platform, os
print('Python:', sys.version)
print('Platform:', platform.platform())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## SET VARIABLES

# PARAMETERS CELL ********************

workspace_name = "demoworkspace12" #what you want the new workspace that is created to be names
varRepo="paulshaheen" #the repo owner in GIT you want to use
varRepoName="fabric-solution-accelerator" #the specific repo in GIT
varBranch="main"
varAdminID = "895e0a62-489f-444a-9b36-322fb8a7f795"
varFolder ="fabric_items" #the folder in the repo that contains your fabric artifacts to be deployed
capacityID = "1db1d7e7-c9d2-4876-ab5c-2681738e0d88" #the capacity to assign the workspace to.  use the cell 2 down from here to list capacities if you do not know the id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Confirm Successful Lib installs

# CELL ********************

# --- SEMPy ---
import sempy
import notebookutils
import sempy.fabric as fabric
from fabric_launcher import FabricLauncher

try:
    from sempy.fabric import FabricRestClient
except Exception as e:
    FabricRestClient = None
    print('FabricRestClient import not available in this SEMPy version:', e)

# --- Fabric Launcher ---
try:
    from fabric_launcher import FabricLauncher
except Exception as e:
    FabricLauncher = None
    print('FabricLauncher import not available (adjust import/package name):', e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## List Capacities and assign the ID to the subsequent cell

# CELL ********************

capacities = fabric.list_capacities()
capacities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Create the new empty workspace DO NOT FORGET TO UPDATE CAPACITY ID
# If you already have an empty workspace skip this and set the ID

# CELL ********************

client = fabric.FabricRestClient()

payload = {
    "displayName": workspace_name,
    # Optional if you want to assign it immediately:
    "capacityId": capacityID
}

# Create Workspace is POST /v1/workspaces
resp = client.post("/v1/workspaces", json=payload, lro_wait=True)

print(resp.status_code)
print(resp.json())
new_workspace_id = resp.json().get("id")
print("New WORKSPACE_ID:", new_workspace_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Apply network policy to new workspace

# CELL ********************

def networkUpdate(workspace_id: str,outbound:str,inbound:str):


    """
    proper values are either Allow or Deny
    Safe even when ETag is not returned by GET.
    """

    client = fabric.FabricRestClient()

    path = f"v1/workspaces/{workspace_id}/networking/communicationPolicy"

    # 1️⃣ Get existing policy (may NOT return ETag)
    r = client.get(path)
    r.raise_for_status()
    policy = r.json()

    # 2️⃣ Ensure required structure exists
    policy.setdefault("inbound", {}).setdefault(
        "publicAccessRules", {}
    ).setdefault("defaultAction", inbound)

    policy.setdefault("outbound", {}).setdefault(
        "publicAccessRules", {}
    )["defaultAction"] = outbound

    # 3️⃣ PUT without If-Match (allowed)
    r2 = client.put(path, json=policy)
    r2.raise_for_status()

    return {
        "workspaceId": workspace_id,
        "status": f"Outbound public access set to {outbound}.  Inbound Access set to {inbound}",
        "appliedPolicy": policy
    }


result = networkUpdate(new_workspace_id,"Allow","Allow") #Allow outbound and allow inbound.  API does not support private end points yet
result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Deploy the workspace to Fabric

# CELL ********************



# If you want to explicitly target a different workspace than the notebook's workspace:
TARGET_WORKSPACE_ID = new_workspace_id  # or "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

launcher = FabricLauncher(
    notebookutils=notebookutils,
    workspace_id=TARGET_WORKSPACE_ID,      # None => auto-detect current workspace
    #environment="DEV",                     # DEV / TEST / PROD
    debug=True,
    allow_non_empty_workspace=False,         # set False to prevent accidental overwrite
    fix_zero_logical_ids=True,
    #config_file="Files/config/deployment_config.yaml"  # local path
)

# Deploy using config

launcher.download_and_deploy(
    repo_owner= varRepo,
    repo_name=varRepoName,
    branch=varBranch,
    workspace_folder=varFolder
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Add the SP and User specified as admin to the newly provisioned workspace
# Note that only an ADMIN ID is being added.  This will need to be updated in the future when a service principal is used

# CELL ********************

payload = {
    "principal": {
        "id": varAdminID,  # Entra Object ID
        "type": "User"  # User | Group | ServicePrincipal
    },
    "role": "Admin"
}

client.post(
    f"/v1/workspaces/{new_workspace_id}/roleAssignments",
    json=payload
).raise_for_status()

print("✅ User added to workspace")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Use Fabric Rest Client to refresh the semantic models
# Required to bind data to newly created semantic model

# CELL ********************

resp = client.get(f"/v1/workspaces/{new_workspace_id}/semanticModels")
resp.raise_for_status()

semantic_models = resp.json()["value"]

for m in semantic_models:
    print(f"{m['displayName']}  |  id={m['id']}  |  Refreshing")
    fabric.refresh_dataset(workspace=workspace_name, dataset=m['displayName'])
dataset_name=m['displayName']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


df = fabric.evaluate_dax(
    dataset=dataset_name,
    dax_string="EVALUATE TOPN(5, 'accounts')"
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Next steps
# - Add your `parameters.yml` mapping file
# - Add post-deployment steps (seed data load, refreshes, parameter updates)
# - Package this notebook + solution folder as a shareable accelerator

