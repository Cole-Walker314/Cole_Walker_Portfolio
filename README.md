#  📃 [Cole_Walker_Portfolio](https://github.com/Cole-Walker314/Cole_Walker_Portfolio)
This repository is used to store portfolio projects from Cole Walker

# 📊 [Fleet Foxes Spotify Pipeline](https://github.com/Cole-Walker314/Cole_Walker_Portfolio/tree/main/fleet-foxes-spotify-pipeline)

A data engineering portfolio project that automates the extraction and transformation of Fleet Foxes Spotify data using Apache Airflow, and visualizes the results in Power BI.

## 🎯 Project Overview

This project demonstrates how to build a simple data pipeline that:
1. **Extracts** Spotify track and album data for Fleet Foxes using the Spotify Web API on a daily basis.
2. **Transforms** the raw JSON data into pandas DataFrames using Apache Airflow tasks.
3. **Loads** the processed data into a local PostgreSQL database , provided by the Astro CLI.
4. **Visualizes** the data in a Power BI report by:
   - Comparing tracks and albums by popularity.
   - Utilizing KPI cards and DAX measures to display insightful metrics.

## ⚙️ Tech Stack

- **Apache Airflow** - Workflow orchestration
- **PostgreSQL** - Data storage
- **Spotify API** - Data source
- **Power BI** - Data visualization
- **Python** - ETL scripting

![](/images/spotify-report-page.png)
