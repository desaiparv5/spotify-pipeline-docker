FROM python:3.12.7

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py .
COPY run.sh .
COPY yacron.conf .

RUN chmod +x run.sh

CMD ["./run.sh"]
