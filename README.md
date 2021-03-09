This repository can be used to set up and deploy instances of online machine learning models, to generate predictions and update the model weights on streaming data.  

High-level steps to set up, deploy, run, and evaluate online learning experiments: 

1. Deploy infrastructure to host experiments (for our purposes this was done on the AWS cloud).  
2. Set up Kafka to generate data streams (this can be done with a managed Kafka service such as Confluent Kafka, although any Kafka solution should work). You will need both topics of data to read from (we expect 8 partitions of the data stream) as well as topics where you can write results. 
3. [Optional] Set up or gain access to Domino environment (alternatives can be set up using other solutions).  
4. Connect your compute/model instances to your Kafka cluster. 
5. ...
