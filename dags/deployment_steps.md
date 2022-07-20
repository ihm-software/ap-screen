# Deployment Steps for Windows

### Step 1: After cloning the repo, navigate to local repo.
**example:** _cd Desktop/test/ap-screen_
_______________________
### Step 2: Build the docker image.
**example:** _docker build -i <image_name> ._
_______________________
### Step 3: Go to docker desktop to find image id.
**example:** _next to image name "ihm", id is 5f581d89c0a4_
_______________________
### Step 4: Run a container using image and mount the dags folder as a volumne (ensure you specify the location of the folder for Airflow Web UI to read DAG correctly).
**example:** _docker run -d -it -v"C:\Users\ULONGT6\Desktop\test\ap-screen\dags":/opt/airflow/dags -p 8080:8080 --name <container_name> <image_id>_
_______________________
### Step 5: Enter terminal for execution of commands within the container.
**example:** _docker exec -t -i <container_name> /bin/bash_
_______________________
### Step 6: Create Airflow user to allow login once Airflow Web UI is entered.
**example:** _airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin_
_______________________
### Step 7: Initialize database backend in Airflow (this is necessary for Web UI to function).
**example:** _airflow db init_
_______________________
### Step 8: Start the Airflow webserver.
**example:** _airflow webserver_
_______________________
### Step 9: Open a new command prompt while webserver is running to execute commands within docker container.
**example:** _open new command prompt and run command docker exec -t -i <container_name> /bin/bash_
_______________________
### Step 10: In the same new command prompt, begin running the airflow scheduler to read and schedule new DAGs.
**example:** _airflow scheduler_
_______________________
### Step 11: Navigate to Airflow Web UI and log in.
**example:** _go to localhost:8080 on browser and use username "admin" and password "admin"_
_______________________
### Step 12: Go to Screening-DAG and trigger in top right corner. All questions are answered in one DAG.
**NOTE:** _I was not able to connect to my local MySQL database, and was thus not able to test my code. However, the DAG does not throw any high-level errors and should function after some debugging._