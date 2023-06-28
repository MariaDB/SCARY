FROM python:3

WORKDIR /processor

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "./processor.py" ]

