import pandas as pd
import re
from datetime import datetime, time
from typing import List, TypedDict, Optional

pd.set_option('display.max_colwidth', None)

ALL_DAYS_LIST = ["Mon", "Tues", "Wed", "Thu", "Fri", "Sat", "Sun"]

file = "liine_data.txt"

def parse_time_alt(time_str: str) -> time:
    time_str = time_str.strip()
    fmt = "%I:%M %p" if ":" in time_str else "%I %p"
    return datetime.strptime(time_str, fmt).time()

def get_next_day(day: str) -> str:
    if day not in ALL_DAYS_LIST:
        return ""

    index = ALL_DAYS_LIST.index(day) + 1
    if index == len(ALL_DAYS_LIST):
        index = 0
    return ALL_DAYS_LIST[index]

def extract_days(days_string: str) -> List[str]:
    all_days_list_regex = "(?:" + "|".join(ALL_DAYS_LIST) + ")"
    extract_days_regex = f"(?:{all_days_list_regex}-{all_days_list_regex})|{all_days_list_regex}"

    raw_days = re.findall(extract_days_regex, days_string)
    days = []

    for day in raw_days:
        if "-" in day:
            day_range = day.split("-")
            start_day = day_range[0]
            end_day = day_range[1]

            start_day_index = ALL_DAYS_LIST.index(start_day)
            end_day_index = ALL_DAYS_LIST.index(end_day) + 1

            day_chunks = ALL_DAYS_LIST[start_day_index:end_day_index]
            days.extend(day_chunks)
        else:
            days.append(day)

    return days

def extract_times(time_string: str) -> TypedDict:
    try:
        times_list = time_string.split(" - ")
        if len(times_list) != 2:
            raise ValueError(f"Invalid time format: {time_string}")

        open_time = parse_time_alt(times_list[0])
        close_time = parse_time_alt(times_list[1])

        return {"open_time": open_time, "close_time": close_time}
    except ValueError as e:
        raise ValueError(f"Invalid time string format: {time_string}") from e
    
def midnight_crossover_etl(restaurant_hours_old: List[TypedDict]) -> List[TypedDict]:
    restaurant_hours_new = []
    for hours in restaurant_hours_old:
        if hours['close_time'] < datetime.strptime("11:59", "%H:%M").time():
            temp = hours.copy()
            temp['close_time'] = datetime.max.time()
            restaurant_hours_new.append(temp)
            midnight_crossover_data = {"day": get_next_day(temp['day']), "open_time": datetime.min.time(), "close_time": hours["close_time"]}
            restaurant_hours_new.append(midnight_crossover_data)
        else:
            restaurant_hours_new.append(hours)

    return restaurant_hours_new
    
def restaurant_hours_etl(x: str) -> List[TypedDict]:
    days_times_open = []

    for schedule_chunk in x.split("/"):
        main_content = schedule_chunk.strip().replace(", ", ",").split(" ")
        days_string = main_content[0]
        time_string = " ".join(main_content[1:])
        
        days = extract_days(days_string)
        times = extract_times(time_string)

        for day in days:
            days_times_open.append({"day": day, "open_time": times["open_time"], "close_time": times["close_time"]})

    return midnight_crossover_etl(days_times_open)

def main_etl(data: pd.DataFrame) -> pd.DataFrame:
    try:
        processed_data = data.copy()
        processed_data["open_times_normalized"] = processed_data['Hours'].apply(restaurant_hours_etl)
        data_expanded = (processed_data.explode('open_times_normalized')
                    .reset_index(drop=True)
                    .join(pd.json_normalize(processed_data['open_times_normalized'].explode())))

        del data_expanded['open_times_normalized']

        return data_expanded
    except Exception as e:
        raise RuntimeError(f"ETL process failed: {str(e)}") from e

def is_open(open_time: time, close_time: time, current_time: time) -> bool:
    return open_time <= current_time <= close_time

def search_open_restaurants(
    data: pd.DataFrame,
    datetime_str: str,
    restaurant: Optional[str] = None
) -> List[str]:
    try:
        dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
        search_day = dt.strftime("%A")[:3]
        search_time = dt.time()

        if search_day == "Tue":
            search_day = "Tues"

        filtered_data = data.copy()
        is_open_day = (filtered_data["day"]==search_day)
        filtered_data = filtered_data[is_open_day]

        is_open_hour = filtered_data.apply(
            lambda row: is_open(row["open_time"], row["close_time"], search_time), 
            axis=1
        )
        if restaurant:
            is_restaurant = (filtered_data["Restaurant Name"]==restaurant)
            filtered_data = filtered_data[is_restaurant]

        return filtered_data.loc[is_open_hour, "Restaurant Name"].drop_duplicates().tolist()
    
    except ValueError as e:
        raise ValueError(f"Invalid datetime format. Expected 'YYYY-MM-DD HH:MM', got '{datetime_str}'") from e
