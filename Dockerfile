from debian:buster as tools

run apt-get update && apt-get install -y \
	git \
	curl \
	jq

from apache/airflow
copy --from=tools /usr/lib /usr/lib
copy --from=tools /usr/bin /usr/bin
#copy --from=tools /usr/share /usr/share
EXPOSE 8080
run airflow db init
workdir /opt/airflow

run ["/bin/bash", "-c", "mkdir data && cd data && while read i; do git clone $i; done < <(curl -s https://api.github.com/orgs/datasets/repos?per_page=100 | jq -r '.[].clone_url')"]

entrypoint ["bash"]
