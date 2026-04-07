# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

ARG API_KEY
# Assign it to an environment variable
ENV GOOGLE_API_KEY=${API_KEY}
ENV GOOGLE_GENAI_USE_VERTEXAI=FALSE

# Install system dependencies for authentication
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
# Ensure "google-adk[a2a]", "python-dotenv", and "uvicorn" are listed
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Cloud Run expects a service to listen on the port defined by the $PORT env var
# We use uvicorn to serve the 'a2a_app' from your agent.py file
CMD uvicorn agent:a2a_app --host 0.0.0.0 --port ${PORT:-8080}
