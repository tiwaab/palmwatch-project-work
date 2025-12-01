# palmwatch-contributions
Personal contributions to the PalmWatch project: scraping, cleaning, and linking global palm oil supply chain data to support worker grievance resolution.

## About This Repository
This repository contains my personal contributions to the **PalmWatch** project. PalmWatch works to monitor palm oil supply chains and facilitate the resolution of grievances raised by farm workers on plantations around the world. My work focuses on transforming raw supply chain data into clean, structured datasets that can be used for entity resolution and analysis, supporting PalmWatch’s mission to improve transparency and accountability in the industry.

## Overview of Work

The code in this repository includes:

### 1. Data Scraping
- Automated scraping of RSPO member companies and their subsidiaries using Selenium and BeautifulSoup.  
- Downloading and preprocessing the Universal Mill List from the Rainforest Alliance.

### 2. Data Cleaning & Normalization
- Cleaning and standardizing company and mill names, parent companies, and country fields.  
- Assigning unique IDs for entities and preparing structured outputs for further analysis.

### 3. Entity Resolution & Classification
- Using record linkage and custom logic to classify entities as Parent Companies, Groups, or Mills.  
- Deduplicating and combining data from multiple sources into a clean, consolidated dataset.

## Example Files

- `supply_chain_cleaning.py` – Functions for cleaning raw RSPO and UML data.  
- `process_supply_chain.py` – Processing pipeline that cleans, classifies, and consolidates entities.  
- `scraper.py` – Scraper for RSPO members and UML, including automated downloading, parsing, and handling of multiple pages.

## Tools & Libraries

Python, pandas, BeautifulSoup, Selenium, recordlinkage, hashlib
