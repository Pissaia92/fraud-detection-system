FROM python:3.9-slim

# dir
WORKDIR /app

# files
COPY requirements.txt .
COPY main.py .

# depends.
RUN pip install --no-cache-dir -r requirements.txt

# script command w/ container starts
CMD ["python", "main.py"]