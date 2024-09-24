FROM apache/airflow:2.7.1
USER root
# FROM python:3.10

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY . /data-lake-house
WORKDIR /data-lake-house

RUN pip install --no-cache-dir -r requirements.txt
