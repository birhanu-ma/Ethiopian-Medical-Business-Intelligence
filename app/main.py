import uvicorn
from fastapi import FastAPI
from app.api.routes import api_router
from fastapi.responses import RedirectResponse
app = FastAPI(
    title="Medical Data Warehouse API",
    description="Analytical endpoints for medical Telegram data and image analysis.",
    version="1.0.0"
)
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")
# Include the master router that aggregates all endpoints.
app.include_router(api_router, prefix="/api/v1")
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)