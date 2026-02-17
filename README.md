![Python Pipeline CI](https://github.com/https://github.com/birhanu-ma/https://github.com/birhanu-ma/Ethiopian-Medical-Business-Intelligence.git/actions/workflows/unittest.yml/badge.svg)
# Ethiopian-Medical-Business-Intelligence
Medical-Data-Scraper
Medical Telegram Warehouse: End-to-End ELT Pipeline This project is a robust data engineering platform designed to scrape, transform, and enrich data from Ethiopian medical Telegram channels. By leveraging an ELT (Extract, Load, Transform) architecture, we turn unstructured social media messages into a structured, analytical Data Warehouse.

🏗 System Architecture The pipeline follows a modern data stack approach:

Extract & Load: Python & Telethon scrape raw data into a PostgreSQL "Raw" schema.

Transform: dbt (Data Build Tool) cleans and models data into a Dimensional Star Schema.

Enrich: YOLOv8 performs object detection on scraped images to categorize medical content.

Orchestrate: Dagster manages the end-to-end workflow (In Progress).

💎 Dimensional Data Model (Star Schema) To address complex business questions regarding posting trends and channel activity, we implemented a Star Schema in the marts layer.

Fact Table fct_messages: The central table containing engagement metrics (views, forwards), message_length, and foreign keys to all dimensions.

Dimension Tables dim_channels: Comprehensive metadata for each channel including total_posts, avg_views, and channel_type.

dim_dates: (New) Granular time dimension supporting analysis by day_of_week, is_weekend, month, and year.

dim_images: (Planned) Image classifications derived from YOLOv8 enrichment.

🚀 Getting Started

Environment Setup Clone the repository and set up a virtual environment:
PowerShell

python -m venv .venv .venv\Scripts\activate pip install -r requirements.txt 2. Configure Credentials Create a .env file in the root directory:

Code snippet

TG_API_ID=your_api_id TG_API_HASH=your_api_hash DB_HOST=localhost DB_NAME=medical_warehouse DB_USER=birhanu DB_PASS=your_password 3. Running the Pipeline Step 1: Scrape and Load Raw Data

PowerShell

python src/scraper.py python scripts/load_to_postgres.py Step 2: Transform with dbt

PowerShell

cd medical_warehouse dbt run --profiles-dir . dbt test --profiles-dir . 🛠 Data Transformation & Quality (dbt) We use dbt to ensure our "Data Factory" produces reliable insights.

Staging Layer: Standardizes field names and handles type-casting (e.g., timestamp conversion).

Data Quality Tests:

unique and not_null on message_id.

relationships tests to ensure referential integrity between the Fact and Dimension tables.

Custom Test: assert_positive_views.sql to ensure data sanity.

📈 Future Roadmap Task 3: YOLO Enrichment - Automating the detection of "bottles," "pills," and "labels" to categorize image-heavy posts.

Task 4: Analytical API - Developing FastAPI endpoints to serve the "Top 10 Products" report.

Task 5: Dagster Orchestration - Moving from manual scripts to a fully automated, daily-scheduled pipeline.