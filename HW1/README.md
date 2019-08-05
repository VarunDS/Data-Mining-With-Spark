# Data-Mining-With-Spark

Task1: Data Exploration

I explore the dataset, review.json, containing review information, and write a program to automatically answer the following questions:
A. The total number of reviews
B. The number of reviews in 2019
C. The number of distinct users who wrote reviews
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
E. The number of distinct businesses that have been reviewed
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had

Run command: spark-submit Karan_Maheshwari_task1.py <input_file_name> <output_file_name>

Task2: Partition

Since processing large volumes of data requires performance decisions, properly partitioning the data for processing is imperative.
In this task, I show the number of partitions for the RDD used for Task 1 Question F and the number of items per partition. 
Then, I use a customized partition function to improve the performance of map and reduce tasks. 
A time duration (for executing Task 1 Question F) comparison between the default partition and the customized partition 
(RDD built using the partition function) is also shown in the results.

Run command: spark-submit Karan_Maheshwari_task2.py <input_file_name> <output_file_name> n_partition

Task3: Exploration on Multiple Datasets

In task3, I explore two datasets together containing review information (review.json) and business information (business.json) and 
write a program to answer the following questions:
A. What is the average stars for each city?
B. Use two ways to print top 10 cities with highest stars. I compare the time difference between two methods and explain the 
result. 
- Method1: Collect all the data, and then print the first 10 cities
- Method2: Take the first 10 cities, and then print all

Run using: spark-submit Karan_Maheshwari_task3.py <input_file_name1> <input_file_name2> <output_file_name1> <output_file_name2>
