# Fabric Solution – Deployment Guide

This repository contains a **deployable Microsoft Fabric solution** using **Fabric Launcher** and **SEMPy**.

## Prerequisites

- Microsoft Fabric workspace access (Contributor or higher)
- Fabric Launcher library available in your tenant
- SEMPy available in Fabric notebooks

## Deployment (DEV)
1. Create a new Workspace in Fabric that you intend to use for Admin activities.  Ensure it is assigned to a Fabric Capacity.
2. Click Workspace settings (top right) then Git integration from the menu on the left.
3. Select GitHub
4. Click Add account and provide a name for the git connection
5. Paste the Personal access token - UPDATE THIS
6. Paste the repository URL - https://github.com/paulshaheen/fabric-solution-accelerator
7. Click Connect
8. Select Main from branch (note you may have to scroll down to see it)
9. Type "admin" (no quotes) in the Git folder text box (leave blank if you want all the artifacts synced to your workspace and not just the admin utils)
10. Go back to workspace settings -> GIT Integration -> Disconnect workspace

## Usage
1. fabric_launcher_sempy_sample provisions new workspaces with specific defaults
2. OneLake_Logging_Setup parses raw data from other workspaces for centralized diagnostics in the workspace you provisioned.

## Post‑Deployment Validation
Script at bottom of install notebook confirms everything binds properly

## Environments
- `parameters.dev.yml` – Development
- Add `parameters.test.yml`, `parameters.prod.yml` as needed

## Upcoming
- FabricAdmin data pipeline which calls the fabric notebook and executed under a service principal
- Easy to use Power App which makes an HTTP call to the pipeline to provision workspaces with all appropriate parameters 
