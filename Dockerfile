FROM apache/airflow:2.10.2-python3.11

USER root

RUN apt-get update

RUN apt-get install -y gcc python3-dev

RUN apt-get install -y openjdk-17-jdk

RUN apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install apache-airflow==2.10.2

RUN pip install apache-airflow-providers-openlineage==1.12.2

RUN pip install apache-airflow-providers-google==10.24.0

RUN pip install apache-airflow-providers-apache-spark==4.11.1

RUN pip install pyspark==3.4.2

# RUN pip install torch==2.5.1

# RUN pip install transformers==4.46.3

# RUN pip install sentencepiece==0.2.0

# RUN pip install sacremoses==0.1.1

RUN pip install matplotlib