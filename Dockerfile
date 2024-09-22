FROM apache/airflow:2.10.1

USER root

RUN sudo apt update && \
    sudo apt install -y openjdk-17-jdk && \
    sudo apt-get clean;
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME
RUN pip3 install apache-airflow-providers-apache-spark
RUN pip3 install pandas

USER airflow