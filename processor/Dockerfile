FROM python:3

WORKDIR /processor

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

CMD [ "python", "./processor.py" ]

