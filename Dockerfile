FROM python:3.6-alpine

ADD . /code/
RUN find /app -name "*.pyc" -type f -delete | xargs rm -rf


RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

WORKDIR /code

CMD ["python", "/code/worker.py"]