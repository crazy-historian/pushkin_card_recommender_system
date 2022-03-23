FROM python:3.8
COPY requirements.txt .
COPY run.sh .
RUN pip install -r requirements.txt