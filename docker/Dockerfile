FROM python:3.11-slim

# Install Java (for Spark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean;

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set working directory
WORKDIR /app

# Copy code
COPY . /app
