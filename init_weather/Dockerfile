FROM python:3.8-slim

COPY requirements.txt requirements.txt
# RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev 
# RUN apk add py2-numpy@community py2-scipy@community py-pandas@edge
RUN pip install psycopg2-binary
RUN pip install -r requirements.txt
WORKDIR /app
RUN pip install update
COPY ./app .

CMD ["python", "main.py"]