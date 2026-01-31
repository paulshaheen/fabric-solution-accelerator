# Fabric Solution – Deployment Guide

This repository contains a **deployable Microsoft Fabric solution** using **Fabric Launcher** and **SEMPy**.

## Prerequisites
- Microsoft Fabric workspace access (Contributor or higher)
- Fabric Launcher library available in your tenant
- SEMPy available in Fabric notebooks

## Repository Structure
```
parameters/
  parameters.dev.yml
installer/
  install_solution.ipynb
  post_deploy_validation.ipynb
fabric_items/
  semantic_models/
  notebooks/
```

## Deployment (DEV)
1. Open **installer/install_solution.ipynb** in a Fabric notebook
2. Update environment variables or paths as needed
3. Run all cells to deploy Fabric items

## Post‑Deployment Validation
After deployment, run:
```
installer/post_deploy_validation.ipynb
```
This validates:
- Workspace connectivity
- Semantic model availability
- Sample DAX execution via SEMPy

## Environments
- `parameters.dev.yml` – Development
- Add `parameters.test.yml`, `parameters.prod.yml` as needed
