#Instructions

## Step 1
- After cloning ap-screen repo, change ditectory to ap-screen in your local machine.
- Run the following command to create image(mine is named tdi).

	docker build -t tdi .

## Step 3 
### Mount dags folder and build container
- find out image id by using the following command
	docker ps
- use the following command to build your container(mine is named workbench) and mount dags folder(-v dags:/dags)
	run -d -it -v dags:/dags --name workbench 356307c6d13e

## Step 4
### Enter your container terminal and start webserver instance
- enter container terminal by using the following command

	docker exec -t -i workbench  /bin/bash

- while inside container terminal use the following command to start webserver instance

	airflow webserver