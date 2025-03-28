# Use an official lightweight Python image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy only the necessary files to install dependencies first
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Expose Flask port
EXPOSE 3000

# Start both the Flask app and worker processes using Supervisor
CMD ["sh", "-c", "supervisord -c /app/supervisord.conf"]
