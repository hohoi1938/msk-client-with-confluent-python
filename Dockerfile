FROM python:3
USER root

RUN apt-get update && \
    pip install --upgrade pip && \
    pip install setuptools

COPY . /root/msk-client/

RUN pip install -r /root/msk-client/requirements.txt