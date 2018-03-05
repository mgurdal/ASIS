FROM python:3.5
RUN set -ex && pip install pipenv --upgrade
RUN set -ex && mkdir /app
WORKDIR /app

RUN apt update && apt install libgdal-dev -y
COPY . /app
RUN set -ex && pipenv install --skip-lock

CMD ["bash"]
