from pydantic import BaseModel
from typing import List, Optional

class TopProduct(BaseModel):
    product_name: str
    mention_count: int

class ChannelActivity(BaseModel):
    message_date: str
    post_count: int

class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    view_count: int

class VisualStats(BaseModel):
    image_category: str
    avg_views: float
    total_images: int