FROM python:3.7.9-buster

COPY ./ ./app

ENV PYTHONPATH=/app
