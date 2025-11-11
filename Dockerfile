FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "whale_tracker.py", "--coins", "all", "--threshold", "1000000", "--pnl-period", "day"]
