FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY ./requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

EXPOSE 80

COPY . /app

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "80"]