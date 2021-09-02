FROM python:3.7.2-stretch

COPY . /app
WORKDIR /app

RUN python3 -m pip install -r requirements.txt
ENTRYPOINT ["python3" ,"/app/doughnut.py"]
