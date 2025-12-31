FROM python:3.10-slim

WORKDIR /app

RUN pip install flask pymongo

COPY app.py .

EXPOSE 5001

CMD ["python", "app.py"]