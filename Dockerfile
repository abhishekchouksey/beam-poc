FROM python:3.11-slim

WORKDIR /usr/src/app

COPY requirements*.txt  .
RUN pip install --no-warn-script-location -r requirements_dev.txt

COPY . .

CMD ["python", "data_flow_script.py"]

