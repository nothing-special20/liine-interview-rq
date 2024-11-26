import pandas as pd
import re
from datetime import datetime

pd.set_option('display.max_colwidth', None)

file = "liine_data.txt"

def parse_time_alt(time_str):
    time_str = time_str.strip()
    fmt = "%I:%M %p" if ":" in time_str else "%I %p"
    return datetime.strptime(time_str, fmt).time()

def extract_days(days_string):
    all_days_list = ["Mon", "Tues", "Wed", "Thu", "Fri", "Sat", "Sun"]
    all_days_list_regex = "(?:" + "|".join(all_days_list) + ")"
    extract_days_regex = f"(?:{all_days_list_regex}-{all_days_list_regex})|{all_days_list_regex}"

    raw_days = re.findall(extract_days_regex, days_string)
    days = []

    for day in raw_days:
        if "-" in day:
            day_range = day.split("-")
            start_day = day_range[0]
            end_day = day_range[1]

            start_day_index = all_days_list.index(start_day)
            end_day_index = all_days_list.index(end_day) + 1

            day_chunks = all_days_list[start_day_index:end_day_index]
            days.extend(day_chunks)
        else:
            days.append(day)

    return days

def extract_times(time_string):
    try:
        times_list = time_string.split(" - ")

        open_time = parse_time_alt(times_list[0])
        end_time = parse_time_alt(times_list[1])

        return {"open_time": open_time, "end_time": end_time}
    except ValueError as e:
        raise ValueError(f"Invalid time string format: {time_string}") from e

def restaurant_hours_etl(x):
    days_times_open = []

    for schedule_chunk in x.split("/"):
        main_content = schedule_chunk.strip().replace(", ", ",").split(" ")
        days_string = main_content[0]
        time_string = " ".join(main_content[1:])
        
        days = extract_days(days_string)
        times = extract_times(time_string)

        for day in days:
            days_times_open.append({"day": day, "open_time": times["open_time"], "end_time": times["end_time"]})

    return days_times_open

def main_etl(data):
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

def search_open_times(data, datetime_str):
    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    weekday = dt.strftime("%A")[:3]
    time = dt.time()

    if weekday == "Tue":
        weekday = "Tues"

    filtered_data = data[(data["day"]==weekday) & (data["open_time"] < time) & (data["end_time"] > time)]

    return filtered_data


if __name__ == "__main__":  
    raw_data = pd.read_csv(file)
    main_data = main_etl(raw_data)

    datetime_str = "2024-11-25 01:35"

    x = search_open_times(main_data, datetime_str)

    print(x)

    main_data.to_csv("test_data.csv")
