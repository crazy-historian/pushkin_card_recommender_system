FROM python:3.8
COPY requirements.txt .
COPY run.sh .
COPY unpack_rars.py .
RUN pip install -r requirements.txt