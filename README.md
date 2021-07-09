# Pagila-Rental-Assignment

## Pipeline Architecture:

![Untitled Diagram (1)](https://user-images.githubusercontent.com/59846364/125049481-054f3200-e06f-11eb-8525-aef8f54ecaf9.png)

## Use Case

Our task here is to achieve the total number of movie rentals rented out in a given week along with the rentals returned in the same given week.
The sample pagila database consist of a rental table which has the listing of all the rentals with its return date, customer_id, inventory_id, etc.
Thereby, we achieve our goal by:

- Creating a new database that will hold the aggregated data from pagila database.
- In this new `rentaldb`, we have created a new table which has the following schema
  - `id`: Auto generated identity key for unique identification of each record.
  - `WeekBeginning`: Starting date of the assumed week (7-day interval)
  - `OutstandingRentals`: Total number of movies out for rental for that given week
  - `ReturnedRentals`: Total number of movie rentals returned in the given week
  - `LastUpdateDate`: Date on which the record was last updated


  
 ## Pipeline Tasks:
 
 ### Task 1: Database Connectivity
 We start by checking if the connection to both source and destination database are successful.
 
 ### Task 2: Aggregating Data
 We aggregated the data based on the use case defined above using the below SQL query:
 
 ```
 SELECT
  DATE(date_trunc('week', rental_date)) AS WeekBeginning,
  COUNT(*) AS OutstandingRentals,
  SUM (CASE WHEN return_date >= date_trunc ('week', rental_date)
  AND return_date <= date_trunc ('week', rental_date) + INTERVAL '7 day'
  THEN 1 ELSE 0 END) AS ReturnedRentals
FROM rental
GROUP BY date_trunc('week', rental_date)
ORDER BY WeekBeginning;
 ```

#### Populating the incremental changes

We have three scenarios for loading data into our rollup database

- **Initial Load:** This occurs when the destination table is empty in other words its loading the data for the first time.
- **New Records:** This occurs when we have data added for new weeks which is not yet loaded into the rollup database.
- **Updates:** This occurs if the existing counts of OutstandingRentals or ReturnedRentals are modified.
  
  
## Airflow:

Here I have utilized Airflow to orchestrate the tasks and also to schedule them in a specified interval.

### Why Airflow?
Its a open source platform used to author workflows as Directed Acyclic Graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

![Capture](https://user-images.githubusercontent.com/59846364/125050396-ea30f200-e06f-11eb-8d26-8a78c276c515.PNG)


# References

- https://www.psycopg.org/install/
- https://airflow.apache.org/docs/apache-airflow/stable/installation.html
- https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions

