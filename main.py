from fastapi import FastAPI, status, Query, Request, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
from datetime import datetime
from contextlib import asynccontextmanager

from restaurant_schedules import search_open_restaurants, main_etl, file


@asynccontextmanager
async def lifespan(app: FastAPI):
    restaurant_schedules_raw = pd.read_csv(file)
    app.state.restaurant_schedules_proc = main_etl(restaurant_schedules_raw)

app = FastAPI(lifespan=lifespan)

@app.get("/open-restaurants")
def search(request: Request, datetime_str: str = Query(default=None, examples="2024-11-26 19:30")):
    if not hasattr(request.app.state, "restaurant_schedules_proc"):
        return JSONResponse(
            content={"error": "Server experienced issue loading restaurant schedule."},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

    try:
        datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid datetime format. Use YYYY-MM-DD HH:MM"
        )

    open_restaurants = search_open_restaurants(request.app.state.restaurant_schedules_proc, datetime_str)

    return JSONResponse(
        content=open_restaurants,
        media_type="application/json",
        status_code=status.HTTP_200_OK
    )