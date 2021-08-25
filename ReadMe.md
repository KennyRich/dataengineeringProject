## Data Engineering Project

The dataset used for this project is found on google big query public dataset,
Hackernews dataset, the stories table. 

Below are the questions answered for the fulfillment of the project. The final project is hosted
on google cloud composer.

1. Where was the article originally published ?
2. How many articles has this author published?
3. How many days have past since the author first published a story?
4. What is their average story score? 
5. How many stories has this author published in the last 30 days?
6. Has this author ever published to this website before?

The process of pulling the data is automated using Apache Airflow and then hosted on Google Cloud Composer.

To run the project, you need to have docker installed on your computer and run the command
`docker compose-up` and navigate to `https://localhost:8080` to run the airflow server and run the DAGs

To shut down the whole process, run the command `docker compose-down` on your terminal