#FROM python:3.8.7
FROM ubuntu:18.04

ENV AIRFLOW_HOME=/opt/airflow
ENV HADOOP_HOME /opt/hadoop
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PYTHONPATH /usr/bin/python3.8

# Update apt packages
RUN apt update
RUN apt upgrade -y


# Install python 3.8
RUN apt install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt install python3.8 -y

# Make python 3.7 the default
#RUN echo "alias python=python3.8" >> ~/.bashrc
#RUN export PATH=${PATH}:/usr/bin/python3.8
#RUN /bin/bash -c "source ~/.bashrc"

#RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1

# Set python3.7 as the default python
#RUN update-alternatives --set python /usr/bin/python3.8

# Install pip
RUN apt install python3-pip -y
RUN python3.8 -m pip install --upgrade pip

# supervisord setup                       
RUN apt-get install -y supervisor python3-setuptools
RUN \
  apt-get install -y \
  ssh \
  rsync \
  vim \
  openjdk-8-jdk

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf


RUN \
  wget http://apache.mirrors.tds.net/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz && \
  tar -xzf hadoop-2.10.1.tar.gz && \
  mv hadoop-2.10.1 $HADOOP_HOME && \
  echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
  echo "PATH=$PATH:$HADOOP_HOME/bin" >> ~/.bashrc

# create ssh keys
RUN \
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
  chmod 0600 ~/.ssh/authorized_keys

# copy hadoop configs
ADD configs/*xml $HADOOP_HOME/etc/hadoop/

# copy ssh config
ADD configs/ssh_config /root/.ssh/config

# copy script to start hadoop
ADD start-hadoop.sh start-hadoop.sh


# Airflow setup                       
RUN python3.8 -m pip install --upgrade pip setuptools wheel
RUN pip3.8 install apache-airflow                       
RUN python3.8 -m pip install cffi
#COPY /dags/response_rate_etl.py $AIRFLOW_HOME/dags/
COPY dag.py $AIRFLOW_HOME/dags/
COPY main.py $AIRFLOW_HOME/dags/scripts/
RUN airflow db init
RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
RUN pip3.8 install apache-airflow-providers-apache-spark 

EXPOSE 8080 8088 50070 50075 50030 50060

CMD ["/usr/bin/supervisord"]
