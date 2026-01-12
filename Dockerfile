# Use python base image
FROM python:3.13-slim-bullseye

COPY requirements.txt .
#update pip & install dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt
