FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libfreetype6-dev \
    libx11-dev \
    libxext-dev \
    libxrender-dev \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    ghostscript \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt


COPY . .

CMD ["python", "report.py"]


