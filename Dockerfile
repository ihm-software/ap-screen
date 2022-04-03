FROM debian:buster as tools

RUN apt-get update && apt-get install -y \
	git \
	curl \
	jq \
	supervisor \
	python3-setuptools \
	vim 

FROM apache/airflow
COPY --from=tools /usr/lib /usr/lib
COPY --from=tools /usr/bin /usr/bin
COPY --from=tools /usr/share /usr/share

ADD supervisord.conf /etc/supervisor/supervisord.conf
ADD trigger_answer_dags.sh trigger_answer_dags.sh
WORKDIR /opt/airflow
ENV AIRFLOW_HOME /opt/airflow
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
RUN mkdir /opt/airflow/logs/supervisor

RUN ["/bin/bash", "-c", "mkdir data && cd data && while read i; do git clone $i; done < <(curl -s https://api.github.com/orgs/datasets/repos?per_page=100 | jq -r '.[].clone_url')"] 
COPY ./dags ./dags

RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
RUN airflow db init

EXPOSE 8080

ENTRYPOINT ["/usr/bin/supervisord"]