import os
import json
import logging
import asyncio
from datetime import datetime
from typing import List, Optional

from telethon import TelegramClient, errors
from .config import settings
from .schemas import TelegramMessage

# Constants
FLOOD_THRESHOLD_SECONDS: int = 86400  # 24 hours
TARGET_MSG_LIMIT: int = 1000          # The number of messages you want per channel
START_DATE_STR: str = "2026-01-18"
class TelegramScraper:
    def __init__(self, session_name: str = 'scraper_session') -> None:
        """Initializes the scraper using centralized settings."""
        self.api_id: int = int(settings.API_ID)
        self.api_hash: str = settings.API_HASH
        self.session_name: str = session_name
        self.client: Optional[TelegramClient] = None
        
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configures logging relative to the project structure."""
        log_dir = os.path.abspath(os.path.join(settings.PROJECT.BASE_DATA_DIR, "..", "logs"))
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            filename=os.path.join(log_dir, "scraping.log"),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    async def initialize(self) -> None:
        """Initializes the Telegram Client session."""
        if not self.client:
            session_path = os.path.join(os.path.dirname(__file__), "..", self.session_name)
            self.client = TelegramClient(session_path, self.api_id, self.api_hash)
            self.client.flood_sleep_threshold = FLOOD_THRESHOLD_SECONDS
            await self.client.start()

    @staticmethod
    def clean_username(username: str) -> str:
        """Utility to strip URLs and symbols from channel names."""
        return (
            username.replace('https://t.me/', '')
            .replace('t.me/', '')
            .replace('@', '')
            .strip()
        )

    async def scrape_channel(self, channel_username: str) -> None:
        """Extracts exactly 1000 messages from the channel."""
        await self.initialize()
        
        clean_name = self.clean_username(channel_username)
        
        # Partition data by current execution date (Data Lake best practice)
        date_folder = datetime.now().strftime('%Y-%m-%d')
        
        # Build paths using the settings config
        image_dir = os.path.join(settings.PROJECT.BASE_DATA_DIR, settings.PROJECT.IMAGE_SUBDIR, clean_name)
        json_dir = os.path.join(settings.PROJECT.BASE_DATA_DIR, settings.PROJECT.JSON_SUBDIR, date_folder)
        
        os.makedirs(image_dir, exist_ok=True)
        os.makedirs(json_dir, exist_ok=True)

        messages_data: List[dict] = []
        images_downloaded: int = 0
        
        print(f"🚀 Scraping {TARGET_MSG_LIMIT} messages for: {channel_username}...")

        try:
            # Task 2 Logic: Use the 'limit' parameter to get 1000 messages
            async for message in self.client.iter_messages(channel_username, limit=TARGET_MSG_LIMIT):
                
                # Map to the structured schema for engineering excellence
                msg_obj = TelegramMessage(
                    message_id=message.id,
                    channel_name=clean_name,
                    message_date=message.date.isoformat() if message.date else None,
                    message_text=message.text or "",
                    views=message.views or 0,
                    forwards=message.forwards or 0,
                    has_media=message.media is not None
                )

                # Handle Image Downloads
                if message.photo:
                    file_name = f"{message.id}.jpg"
                    save_path = os.path.join(image_dir, file_name)
                    
                    try:
                        if not os.path.exists(save_path):
                            await message.download_media(file=save_path)
                        
                        # Save path relative to project root for portability
                        msg_obj.image_path = f"{settings.PROJECT.IMAGE_SUBDIR}/{clean_name}/{file_name}"
                        images_downloaded += 1
                    except Exception as e:
                        logging.error(f"Media error on msg {message.id}: {e}")

                messages_data.append(msg_obj.to_dict())

            # Save the JSON file
            json_file_path = os.path.join(json_dir, f"{clean_name}.json")
            with open(json_file_path, "w", encoding='utf-8') as f:
                json.dump(messages_data, f, indent=4, default=str)
                
            logging.info(f"✅ {clean_name}: Saved {len(messages_data)} msgs and {images_downloaded} imgs")
            print(f"✅ {clean_name}: Collected {len(messages_data)} messages.")

        except errors.FloodWaitError as e:
            logging.warning(f"Flood limit hit! Sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logging.error(f"Critical error scraping {clean_name}: {e}")
            print(f"❌ Error with {channel_username}: {e}")

    async def run(self, channels: List[str]) -> None:
        """Main execution loop for all provided channels."""
        await self.initialize()
        async with self.client:
            for channel in channels:
                await self.scrape_channel(channel)