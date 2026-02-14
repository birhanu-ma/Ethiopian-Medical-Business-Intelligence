import os
import json
import logging
import asyncio
import nest_asyncio
from datetime import datetime
from telethon import TelegramClient, errors

# Apply nest_asyncio for compatibility in Jupyter/async environments
nest_asyncio.apply()

class TelegramScraper:
    def __init__(self, api_id, api_hash, session_name='scraper_session'):
        self.api_id = int(api_id) if api_id else None
        self.api_hash = api_hash
        self.session_name = session_name
        self.client = None
        
        # Project root path
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        
        # Set up logging (Task 1: Implement Logging)
        self.log_dir = os.path.join(self.base_path, "logs")
        os.makedirs(self.log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(self.log_dir, "scraping.log"),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    async def initialize(self):
        """Initializes the Telegram Client"""
        if not self.client:
            session_path = os.path.join(self.base_path, self.session_name)
            self.client = TelegramClient(session_path, self.api_id, self.api_hash)
            self.client.flood_sleep_threshold = 24 * 60 * 60 # Handle long rate limits
            await self.client.start()

    def clean_username(self, username):
        """Removes URL parts to get a clean channel name for folder structures"""
        return username.replace('https://t.me/', '').replace('t.me/', '').replace('@', '').strip()

    async def scrape_channel(self, channel_username, msg_limit=1000):
        """Extracts messages and images into a partitioned Data Lake"""
        await self.initialize()
        
        clean_name = self.clean_username(channel_username)
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        # 1. Image Path: data/raw/images/{channel_name}/{message_id}.jpg
        image_dir = os.path.join(self.base_path, "data", "raw", "images", clean_name)
        
        # 2. JSON Path: data/raw/telegram_messages/YYYY-MM-DD/channel_name.json
        json_dir = os.path.join(self.base_path, "data", "raw", "telegram_messages", date_str)
        
        os.makedirs(image_dir, exist_ok=True)
        os.makedirs(json_dir, exist_ok=True)

        messages_data = []
        images_downloaded = 0
        
        logging.info(f"Starting scrape for {channel_username} (Target: {msg_limit} msgs)")

        try:
            async for message in self.client.iter_messages(channel_username, limit=msg_limit):
                # Build the record while preserving raw API data structure
                msg_entry = {
                    "message_id": message.id,
                    "channel_name": clean_name,
                    "message_date": message.date.isoformat() if message.date else None,
                    "message_text": message.text or "",
                    "views": message.views or 0,
                    "forwards": message.forwards or 0,
                    "has_media": message.media is not None,
                    "image_path": None,
                }

                # Download images if present (Unlimited as requested)
                if message.photo:
                    file_name = f"{message.id}.jpg"
                    save_path = os.path.join(image_dir, file_name)
                    
                    try:
                        if not os.path.exists(save_path):
                            await message.download_media(file=save_path)
                        msg_entry["image_path"] = f"data/raw/images/{clean_name}/{file_name}"
                        images_downloaded += 1
                    except Exception as e:
                        logging.error(f"Media download failed for msg {message.id}: {e}")

                messages_data.append(msg_entry)

            # Save to partitioned JSON file
            json_file_path = os.path.join(json_dir, f"{clean_name}.json")
            with open(json_file_path, "w", encoding='utf-8') as f:
                json.dump(messages_data, f, indent=4, default=str)
                
            logging.info(f"Successfully scraped {len(messages_data)} msgs and {images_downloaded} imgs for {clean_name}")
            print(f"✅ {clean_name}: Saved to {json_file_path}")

        except errors.FloodWaitError as e:
            logging.warning(f"Rate limit hit! Must wait {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logging.error(f"Critical error scraping {clean_name}: {e}")

    async def run(self, channels):
        """Runs the scraper for a list of channels"""
        await self.initialize()
        async with self.client:
            for channel in channels:
                await self.scrape_channel(channel)