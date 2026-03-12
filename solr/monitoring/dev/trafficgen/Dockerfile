FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY trafficgen.py .
COPY trafficgen.yml .

RUN useradd -r -u 1000 -g root trafficgen
USER 1000

ENTRYPOINT ["python3", "trafficgen.py"]
