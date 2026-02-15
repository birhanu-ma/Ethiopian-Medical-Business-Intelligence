import pandas as pd
import json
import glob
from pathlib import Path
from sqlalchemy import text
from transformers import pipeline
from deep_translator import GoogleTranslator
from tqdm import tqdm
from app.db.database import engine as db_engine  # Ensure this engine uses your DATABASE_URL

class TelegramEnrichmentHandler:
    def __init__(self, engine=None):
        """
        Initialize the handler with a SQLAlchemy engine.
        Defaults to the engine defined in app.db.database.
        """
        self.engine = engine or db_engine
        self._initialize_nlp()

    def _initialize_nlp(self):
        """
        Load HuggingFace NLP pipelines.
        """
        print("Loading HuggingFace models... (Downloads ~2GB on first run)")
        self.classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
        self.sentiment_analyzer = pipeline("sentiment-analysis")
        self.category_labels = ["Promotion", "Stock Update", "Educational", "Product Display", "Medical Inquiry"]
        tqdm.pandas(desc="AI Processing")

    def process_message(self, text):
        """
        Process a single message: translate, categorize, and analyze sentiment.
        """
        if not text or not isinstance(text, str) or text.strip() == "":
            return "General", "Neutral", ""
        try:
            translated = GoogleTranslator(source='auto', target='en').translate(text)
            cat_result = self.classifier(translated, self.category_labels)
            category = cat_result['labels'][0]
            tone_result = self.sentiment_analyzer(translated)
            tone = tone_result[0]['label']
            return category, tone, translated
        except Exception:
            return "Uncategorized", "Neutral", text

    def _load_files(self, folder_path):
        """
        Load CSV and JSON files from folder (recursively) into a list of records.
        """
        folder = Path(folder_path)
        if not folder.exists():
            print(f"❌ Error: Folder {folder_path} not found.")
            return []

        all_data = []

        # Load CSV files
        for file in folder.glob("**/*.csv"):
            df = pd.read_csv(file)
            all_data.extend(df.to_dict(orient='records'))

        # Load JSON files
        for file in folder.glob("**/*.json"):
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON file: {file}")

        print(f"Found {len(all_data)} records in folder {folder_path}")
        return all_data

    def run_pipeline(self, folder_path, table_name='telegram_messages', schema='raw'):
        """
        Read files, enrich messages, and upload to PostgreSQL.
        Creates the schema if it does not exist.
        """
        # Ensure schema exists
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        # Load all files
        all_data = self._load_files(folder_path)
        if not all_data:
            print("No data found. Exiting pipeline.")
            return

        # Enrich and upload
        self.enrich_and_upload(all_data, table_name=table_name, schema=schema)

    def enrich_and_upload(self, data_list, table_name='telegram_messages', schema='raw'):
        """
        Enrich data with NLP and upload to PostgreSQL.
        """
        df = pd.DataFrame(data_list)

        if 'message_text' not in df.columns:
            print("❌ Error: 'message_text' column missing from data.")
            return

        df = df[df['message_text'].fillna('').str.strip() != ""].copy()
        print(f"\nStarting enrichment for {len(df)} messages...")

        # NLP Processing
        res = df['message_text'].progress_apply(lambda x: pd.Series(self.process_message(x)))
        df['content_category'] = res[0]
        df['tone_label'] = res[1]
        df['translated_text'] = res[2]

        # Final Category Logic
        df['final_category'] = df.apply(
            lambda row: "Urgent Medical" 
            if row['tone_label'] == 'NEGATIVE' and row['content_category'] == 'Medical Inquiry' 
            else row['content_category'], axis=1
        )

        # Column Mapping to match dbt expectations
        mapping = {
            'message_date': 'message_date',
            'views': 'view_count',
            'has_media': 'has_image',
            'channel_name': 'channel_key'
        }
        df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

        # Ensure message_id is numeric
        if 'message_id' in df.columns:
            df['message_id'] = pd.to_numeric(df['message_id'], errors='coerce')
            df = df.dropna(subset=['message_id'])

        # Upload
        try:
            print(f"Uploading {len(df)} records to {schema}.{table_name}...")
            df.to_sql(table_name, con=self.engine, schema=schema, if_exists='append', index=False)
            print("✅ Upload complete!")
        except Exception as e:
            print(f"❌ Upload failed: {e}")