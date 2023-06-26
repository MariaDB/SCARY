FROM python:3

WORKDIR /agent

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./agent.py" ]

