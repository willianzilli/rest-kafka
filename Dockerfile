FROM tiangolo/uwsgi-nginx-flask:python3.12

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./app /app