# Clustered-data-processing-with-Network-partition

Custer setup of three machines
each machine has Redis, RabbitMQ and Celery

Redis : key-value store
RabbitMQ : message queuing
Celery : worker manager

We have implemented a word-count application for counting the words in the tweets of given dataset.
The data is stored in the Redis store. 
We make available cluster -> at any point data of store is accessible
