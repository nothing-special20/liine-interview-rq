# liine-interview-rq
Python code to transform and search restaurant data for Liine's take home coding exam

# how to run
docker build -t fastapi-app .
docker run -p 8000:8000 fastapi-app

# example curl request
curl "http://localhost:8000/open-restaurants?datetime_str=2024-11-26%2019:30"