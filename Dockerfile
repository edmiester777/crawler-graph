FROM python:3.10

WORKDIR /path
COPY requirements.txt .
COPY src/ .

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "main.py" ]