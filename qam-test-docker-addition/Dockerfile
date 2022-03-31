FROM apache/airflow:2.1.2-python3.9
MAINTAINER Leonard

USER root
WORKDIR /opt/airflow
RUN mkdir qam-test-docker-addition
WORKDIR /qam-test-docker-addition
COPY unrequirements.txt ./
COPY requirements.txt ./
RUN apt-get update
RUN apt-get install -y wget
RUN apt-get install -y vim

USER airflow
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip uninstall -y -r  unrequirements.txt
RUN pip install -r requirements.txt
WORKDIR /opt/airflow

#docker build -t qam-test:latest .