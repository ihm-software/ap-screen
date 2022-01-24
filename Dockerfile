from debian:buster as tools

run apt-get update && apt-get install -y \
	git \
	curl \
	jq

from apache/airflow
copy --from=tools /usr/lib /usr/lib
copy --from=tools /usr/bin /usr/bin
copy --from=tools /usr/share /usr/share

workdir /opt/airflow

run ["/bin/bash", "-c", "mkdir data && cd data && while read i; do git clone $i; done < <(curl -s https://api.github.com/orgs/datasets/repos?per_page=10 | jq -r '.[].clone_url')"] 

entrypoint ["bash"]
