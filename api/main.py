"""
Climate Projections API - Main application entry point.

FastAPI application that serves climate projection data for French addresses.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import climate
import joblib
import logging
from contextlib import asynccontextmanager

from src.patched_lightgbm import PatchedLGBMRegressor

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting API lifespan")

    app.state.temp_max_model = joblib.load("models/temp_max_year.joblib")
    app.state.wind_model = joblib.load("models/temp_max_wind.joblib")
    app.state.rain_model = joblib.load("models/pred_cum_year.joblib")

    yield
    logger.info("🛑 Shutting down API lifespan")

app = FastAPI(
    lifespan=lifespan,
    title="Climate Projections API",
    description="API for retrieving climate projections and commune data for French addresses",
    version="0.1.0"
)

# CORS middleware - Allow Quarto dev server and common ports
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*",
    ],
    allow_credentials=True,
    allow_methods=["GET"],            # Only need GET for this API
    allow_headers=["*"],
)

# Include routers
app.include_router(climate.router, prefix="/api", tags=["climate"])


@app.get("/api/health")
async def health_check():
    """
    Health check endpoint.

    Returns API status and version information.
    """
    return {
        "status": "healthy",
        "version": "0.1.0",
        "service": "Climate Projections API"
    }


@app.get("/")
async def root():
    """
    Root endpoint - redirect to API docs.
    """
    return {
        "message": "Climate Projections API",
        "docs": "/docs",
        "health": "/api/health"
    }
