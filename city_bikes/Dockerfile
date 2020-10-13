FROM python:3.8-slim

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
WORKDIR /app
RUN pip install update
COPY . .

CMD ["python", "bysykkel_data.py"]