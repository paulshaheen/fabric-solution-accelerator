# Fabric Solution – Deployment Guide

This repository contains a **deployable Microsoft Fabric solution** using **Fabric Launcher** and **SEMPy**.

## Prerequisites

- Microsoft Fabric workspace access (Contributor or higher)
- Fabric Launcher library available in your tenant
- SEMPy available in Fabric notebooks

## Deployment (DEV)
1. Download the fabric_launcher_sempy_sample notebook from this repo (root)
2. import the notebook into any Fabric workspace of your choice
3. Update environment variables or paths as needed
4. Run all cells to deploy Fabric items

## Post‑Deployment Validation
Script at bottom of install notebook confirms everything binds properly

## Environments
- `parameters.dev.yml` – Development
- Add `parameters.test.yml`, `parameters.prod.yml` as needed

## Upcoming
- FabricAdmin data pipeline which calls the fabric notebook and executed under a service principal
- Easy to use Power App which makes an HTTP call to the pipeline to provision workspaces with all appropriate parameters 
