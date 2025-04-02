# Use a lightweight Python image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Expose Flask port
EXPOSE 3000

# Start both Flask app and workers.py
CMD ["sh", "-c", "python -m app.utils.workers & gunicorn -w 4 -b 0.0.0.0:3000 app.app:app"]

