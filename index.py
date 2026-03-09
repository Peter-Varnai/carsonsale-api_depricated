from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging

from db import (
    get_cars_by_brand,
    get_cars_by_brands,
    DatabaseError,
    QueryError,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return {"carsonsale.info"}


@app.get("/brands")
def list_brands():
    """Get list of all available car brands."""
    try:
        from db.queries import get_all_brands
        brands = get_all_brands()
        return {"brands": brands}
    except QueryError as e:
        logger.error(f"Database error: {e}")
        return JSONResponse({"error": "Failed to fetch brands"}, status_code=500)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return JSONResponse({"error": "Internal server error"}, status_code=500)


@app.get("/countries")
def list_countries():
    """Get list of all available country codes."""
    try:
        from db.queries import get_distinct_countries
        countries = get_distinct_countries()
        return {"countries": countries}
    except QueryError as e:
        logger.error(f"Database error: {e}")
        return JSONResponse({"error": "Failed to fetch countries"}, status_code=500)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return JSONResponse({"error": "Internal server error"}, status_code=500)


@app.get("/{car_brands}")
def index(car_brands: str):
    """Get cars by brand(s). Multiple brands can be comma-separated."""
    try:
        if "," in car_brands:
            brands_list = [b.strip() for b in car_brands.split(",")]
            cars, cc_list = get_cars_by_brands(brands_list)
        else:
            cars = get_cars_by_brand(car_brands.strip())
            cc_list = list(set(
                car["seller_country_code"] 
                for car in cars 
                if car.get("seller_country_code")
            ))
        
        out = {
            "cc": cc_list,
            "data": cars
        }
        return JSONResponse(out)
        
    except QueryError as e:
        logger.error(f"Database query error: {e}")
        return JSONResponse({"error": "Database query failed"}, status_code=500)
    except DatabaseError as e:
        logger.error(f"Database connection error: {e}")
        return JSONResponse({"error": "Database connection failed"}, status_code=503)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return JSONResponse({"error": "Internal server error"}, status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app)
