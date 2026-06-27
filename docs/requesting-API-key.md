# API Key Provisioning Guide

## Overview

This guide explains how to request an API key for the Netrias Harmonization platform.

API keys are required for:

- Netrias Client access
- Direct REST API access

## Who Can Request a Key?

- BDF performers
- NIH staff
- Third-party integrators, provided a mutual NDA and data-use agreement are in place first

## Request Process

1. **Prepare your details** – Collect the following information:

   | Field                     | Why we need it                                              |
   | ------------------------- | ----------------------------------------------------------- |
   | **Name**                  | Identifies the responsible individual.                      |
   | **Organization / Team**   | Helps us assign the correct quota.                          |
   | **Intended use case**     | Brief description, e.g., “Testing the Netrias Client on XYZ data.” |
   | **Estimated data volume** | Example: 50 CSV, TSV, or Excel (`.xlsx`) files with 3,000 rows each.                 |

2. **Submit your request** – Email the above details to **Netrias** at [bdf_strides@netrias.com](mailto:bdf_strides@netrias.com) with the subject line **“API Key Request – Netrias BDF”**.

3. **Approval and issuance** – Netrias will confirm receipt within *1 business day* and issue:

   - Your **API key**
   - The **expiration / rotation schedule**

   By default, keys do **not** automatically expire at the 12-month mark, but we may email you to confirm whether you are still using the key.

4. **Store the key securely**

   - **Never** commit keys to source control.
   - Rotate immediately if you suspect compromise.

## Netrias Client

For programmatic access, use the **[Netrias Client](https://github.com/netrias/netrias_client)**. The client supports discovery, harmonization, and data model store workflows from Python. Its installation and usage documentation is maintained in the Netrias Client repository.

You will need an API key before using the Netrias Client. Once your key is issued, follow the setup instructions in the client repository.

## Key Lifecycle Management

| Action                | Contact                                                       | Typical SLA                                  |
| --------------------- | ------------------------------------------------------------- | -------------------------------------------- |
| **Rotate / Revoke**   | Email [bdf_strides@netrias.com](mailto:bdf_strides@netrias.com) | 4 business hours                             |
| **Report compromise** | Same as above                                                 | Immediate; key disabled upon receipt          |