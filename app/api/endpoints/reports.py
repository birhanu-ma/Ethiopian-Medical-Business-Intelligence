from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

from app.db.database import get_db
from app.schemas.analytical_reports import (
    TopProduct, 
    ChannelActivity, 
    MessageSearchResult, 
    VisualStats
)

router = APIRouter()

@router.get("/top-products", response_model=List[TopProduct], summary="Get Top Mentioned Products")
def get_top_products(limit: int = 10, db: Session = Depends(get_db)):
    # Note: Using public.fct_messages as confirmed by your \dt command
    query = text("""
        SELECT message_text as product_name, COUNT(*) as mention_count 
        FROM public.fct_messages 
        GROUP BY 1 
        ORDER BY 2 DESC 
        LIMIT :limit
    """)
    result = db.execute(query, {"limit": limit})
    return [{"product_name": row[0], "mention_count": row[1]} for row in result]

@router.get("/channels/{channel_name}/activity", response_model=List[ChannelActivity])
def get_channel_activity(channel_name: str, db: Session = Depends(get_db)):
    query = text("""
        SELECT date_key::text as message_date, COUNT(*) as post_count 
        FROM public.fct_messages 
        WHERE channel_key = (SELECT channel_key FROM public.dim_channels WHERE channel_name = :name LIMIT 1)
        GROUP BY 1 ORDER BY 1
    """)
    result = db.execute(query, {"name": channel_name})
    return [{"message_date": row[0], "post_count": row[1]} for row in result]

@router.get("/search/messages", response_model=List[MessageSearchResult])
def search_messages(query: str, limit: int = 20, db: Session = Depends(get_db)):
    sql = text("""
        SELECT message_id, channel_key as channel_name, message_text, view_count 
        FROM public.fct_messages 
        WHERE message_text ILIKE :search 
        LIMIT :limit
    """)
    result = db.execute(sql, {"search": f"%{query}%", "limit": limit})
    return [
        {"message_id": row[0], "channel_name": str(row[1]), "message_text": row[2], "view_count": row[3]} 
        for row in result
    ]

@router.get("/visual-content", response_model=List[VisualStats])
def get_visual_stats(db: Session = Depends(get_db)):
    # CRITICAL FIX: Pointing to the schema 'public_analytics' created by dbt
    query = text("""
        SELECT 
            image_category, 
            AVG(view_count) as avg_views, 
            COUNT(*) as total_images 
        FROM public_analytics.fct_image_detections 
        GROUP BY 1
    """)
    result = db.execute(query)
    # We use row.image_category, row.avg_views, etc., or indices
    return [
        {
            "image_category": row[0], 
            "avg_views": round(float(row[1] or 0), 2), 
            "total_images": int(row[2])
        } 
        for row in result
    ]