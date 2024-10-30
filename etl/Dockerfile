FROM python:3.9

WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy ETL code
COPY scripts/ ./scripts
COPY tests/ ./tests
COPY data/ ./data
COPY utils/ ./utils

COPY config/ ./config

COPY main.py .

# Set entry point
ENTRYPOINT ["python", "main.py"]
