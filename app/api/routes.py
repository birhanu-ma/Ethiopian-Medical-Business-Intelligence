from fastapi import APIRouter
from app.api.endpoints import reports  # Add analysis here later

api_router = APIRouter()

# We attach the reports router. 
# Now all functions in reports.py will start with /api/v1/reports
api_router.include_router(reports.router, prefix="/reports", tags=["Reports"])