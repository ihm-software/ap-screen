# Instructions

- Build this docker image
- Run a container and _mount_ the folder `dags` as a volume onto the container
- Create a database within the container
- Write some dags that will
  - Load the csv files from the `data` directory into the database you created.
  - Answer the questions [below](README.md#Questions) and output the answers into an answer table for for each question
  - Show the answer to each question in the log view of each dag
- Lastly, create a Pull Request with your code for review
  - We should be able to build this image again, mount the dags folder, and visit the WebUI to see the answers

# Questions

## What's the average number of fields across all the tables you loaded?

Output should be a simple number

_sample output_

```
|Question|Answer|
|--------|------|
| 1      | 5    |
```

## What is the word count of every value of every table

Output should have fields `value` and `count` and one entry for every value found:

_sample output_

```
|     value     | count  |
|---------------|--------|
|   some value  | 435    |
| another value | 234    |
|     word      | 45     |
```

## What's the total number or rows for the all the tables?

Output should be a simple number

_sample output_

```
|Question|Answer|
|--------|------|
| 3      | 1000 |
```
