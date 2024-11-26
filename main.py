from fastapi import FastAPI, status, Query
from fastapi.responses import JSONResponse
import pandas as pd

from liine_tech_take_home_test import search_open_restaurants, main_etl, file

app = FastAPI()

@app.get("/open-restaurants")
def search(datetime_str: str = Query(default=None, example="2024-11-26 19:30")):
    raw_data = pd.read_csv(file)

    main_data = main_etl(raw_data)

    x = search_open_restaurants(main_data, datetime_str)

    return JSONResponse(
        content=x,
        media_type="application/json",
        status_code=status.HTTP_200_OK
    )