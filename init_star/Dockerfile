FROM python:3.8-slim

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
RUN pip install update

CMD ["python", "dbstar_init.py"]