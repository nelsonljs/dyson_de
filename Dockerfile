# syntax=docker/dockerfile:1

FROM python:latest

# Setting up the container's python environment
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Pay attention to the .dockerignore
COPY . .
CMD ["python3", "main.py"]