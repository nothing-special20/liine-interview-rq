import pytest
import pandas as pd
from io import StringIO
from restaurant_schedules import (
    main_etl,
    search_open_restaurants,
)

@pytest.fixture(scope="module")
def test_data():
    csv_data = '''"Restaurant Name","Hours"
"Seoul 116","Mon-Sun 11 am - 4 am"
"Bonchon","Mon-Wed 5 pm - 12:30 am  / Thu-Fri 5 pm - 1:30 am  / Sat 3 pm - 1:30 am  / Sun 3 pm - 11:30 pm"
"The Cheesecake Factory","Mon-Thu 11 am - 11 pm  / Fri-Sat 11 am - 12:30 am  / Sun 10 am - 11 pm"
"42nd Street Oyster Bar","Mon-Sat 11 am - 12 am  / Sun 12 pm - 2 am"
"Garland","Tues-Fri, Sun 11:30 am - 10 pm  / Sat 5:30 pm - 11 pm"'''

    return pd.read_csv(StringIO(csv_data))

@pytest.fixture
def processed_split_data(test_data):
    return main_etl(test_data)

def test_split_hours(processed_split_data):
    assert len(search_open_restaurants(processed_split_data, "2024-11-26 13:30", "Bonchon")) == 0
    assert len(search_open_restaurants(processed_split_data, "2024-11-26 17:00", "Bonchon")) == 1
    assert len(search_open_restaurants(processed_split_data, "2024-11-27 00:00", "Bonchon")) == 1
    assert len(search_open_restaurants(processed_split_data, "2024-11-23 15:00", "Bonchon")) == 1
    assert len(search_open_restaurants(processed_split_data, "2024-11-23 14:59", "Bonchon")) == 0
    assert len(search_open_restaurants(processed_split_data, "2024-11-26 22:01")) == 4