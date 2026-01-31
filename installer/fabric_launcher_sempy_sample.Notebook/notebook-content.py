# Fabric notebook source


# MARKDOWN ********************

# # Fabric Launcher + SEMPy – Sample Installer Notebook
# 
# This notebook is a **starting point** for deploying a Fabric solution from a repo (via **Fabric Launcher**) and then validating / interacting with a semantic model using **SEMPy**.
# 
# **What you’ll do:**
# 1. Install/Import libraries
# 2. Set your workspace + repo parameters
# 3. (Optional) Deploy items using Fabric Launcher
# 4. Discover semantic models and run a simple DAX query
# 
# > Notes: Library names/APIs can vary by version/preview. Adjust imports based on the package versions available in your Fabric tenant.


# MARKDOWN ********************

# ## 0) Environment
# Intended to run **inside a Microsoft Fabric notebook** (or Python notebook experience).


# CELL ********************

import sys, platform, os
print('Python:', sys.version)
print('Platform:', platform.platform())


# MARKDOWN ********************

# ## 1) Install dependencies (if needed)
# If your workspace already has the packages, you can skip this cell.


# CELL ********************

# In Fabric notebooks, %pip is supported
# Uncomment as needed
# %pip install -U sempy
# %pip install -U fabric-launcher


# MARKDOWN ********************

# ## 2) Imports
# These imports are defensive—adjust based on the exact module names you have.


# CELL ********************

# --- SEMPy ---
import sempy

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


# MARKDOWN ********************

# ## 3) Configure your deployment inputs
# Fill these in for your workspace + repo.
# - `WORKSPACE_ID`: target Fabric workspace GUID
# - `REPO_URL`: GitHub/Azure DevOps repo containing the solution
# - `SOLUTION_PATH`: folder in repo that contains Fabric items + config
# - `PARAMETERS_YAML`: path to a YAML file with mapping/replacement rules


# CELL ********************

WORKSPACE_ID = os.getenv('FABRIC_WORKSPACE_ID', '<your-workspace-guid>')
REPO_URL      = os.getenv('FABRIC_SOLUTION_REPO', '<your-repo-url>')
SOLUTION_PATH = os.getenv('FABRIC_SOLUTION_PATH', '<repo-subfolder-or-root>')
PARAMETERS_YAML = os.getenv('FABRIC_PARAMETERS_YAML', 'parameters.yml')

print('WORKSPACE_ID:', WORKSPACE_ID)
print('REPO_URL:', REPO_URL)
print('SOLUTION_PATH:', SOLUTION_PATH)
print('PARAMETERS_YAML:', PARAMETERS_YAML)


# MARKDOWN ********************

# ## 4) (Optional) Deploy solution using Fabric Launcher
# This section assumes you have a deployable solution folder and a YAML parameter file as described in the Fabric Launcher session.
# If the `FabricLauncher` class name or constructor differs in your build, update accordingly.


# CELL ********************

if FabricLauncher is None:
    print('FabricLauncher not available. Install/adjust the package and import.')
else:
    # Example pattern – adjust to match your library
    launcher = FabricLauncher(
        workspace_id=WORKSPACE_ID,
        repo_url=REPO_URL,
        solution_path=SOLUTION_PATH,
        parameters_file=PARAMETERS_YAML
    )
    print('Launcher initialized:', launcher)

    # Preview items (optional)
    try:
        items = launcher.preview_items()
        print('Items discovered:', items)
    except Exception as e:
        print('preview_items() failed (API may differ):', e)

    # Deploy (optional)
    # try:
    #     result = launcher.deploy()
    #     print('Deploy result:', result)
    # except Exception as e:
    #     print('deploy() failed (API may differ):', e)


# MARKDOWN ********************

# ## 5) Use SEMPy to list semantic models and run a basic query
# SEMPy provides a convenient way to work with Power BI / Fabric semantic models from notebooks.
# Below are *example* patterns—adjust for your version of SEMPy.


# CELL ********************

# Example: list semantic models using Fabric REST client (if available)
if FabricRestClient is None:
    print('FabricRestClient not available; use your SEMPy version equivalents.')
else:
    client = FabricRestClient()
    try:
        models = client.list_semantic_models(workspace_id=WORKSPACE_ID)
        print('Semantic models:', models)
    except Exception as e:
        print('Listing semantic models failed (API may differ):', e)


# CELL ********************

# Example: run a DAX query (update to match your SEMPy functions)
DAX_QUERY = '''
EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Year],
    "Total Sales", SUM('Sales'[Sales Amount])
)
'''

# Replace with a real semantic model id/name from the listing above
SEMANTIC_MODEL_ID = '<semantic-model-id>'

try:
    # Placeholder: update to sempy's actual query function in your environment
    from sempy.fabric import execute_dax
    df = execute_dax(workspace_id=WORKSPACE_ID, semantic_model_id=SEMANTIC_MODEL_ID, dax_query=DAX_QUERY)
    display(df.head())
except Exception as e:
    print('DAX execution failed (function/API may differ):', e)


# MARKDOWN ********************

# ## 6) Next steps
# - Add your `parameters.yml` mapping file
# - Add post-deployment steps (seed data load, refreshes, parameter updates)
# - Package this notebook + solution folder as a shareable accelerator

