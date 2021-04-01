# Online Learning Deployment for Streaming Applications

This repository is the official implementation of the paper Online Learning Deployment for Streaming Applications in the Banking Sector.
The ressources can be used to set up and deploy instances of online machine learning models, to generate predictions and update the model weights on streaming data.  

## Deployment Architecture

## Files descriptions

## Pipeline set up & Model Deployment

High-level steps to set up, deploy, run, and evaluate online learning experiments: 

1. Deploy infrastructure to host experiments (for our purposes this was done on the AWS cloud).  
2. Set up Kafka to generate data streams (this can be done with a managed Kafka service such as Confluent Kafka, although any Kafka solution should work). You will need both topics of data to read from (we expect 8 partitions of the data stream) as well as topics where you can write results. 
 
https://github.com/MariamBARRY/streaming-online-learning/blob/main/src/hostedkafka/confluent_producer.py

4. [Optional] Set up or gain access to Domino environment (alternatives can be set up using other solutions).  
5. Connect your compute/model instances to your Kafka cluster.  An example configuration is shown in https://github.com/dominodatalab/streaming-online-learning/blob/main/src/hostedkafka/KafkaConsumer.py. Model instances will pull from a stream on designated topics and write back results on separate topics.
6. Set up the Kafka producer stream on the Kafka end.  This is the stream from which the model instances will pull data for inference and learning.  A producer configuration is demonstrated in https://github.com/dominodatalab/streaming-online-learning/blob/main/src/hostedkafka/confluent_producer.py. 
7. Utilize an appropriate Docker image or virtual environment (or a compute environment if using Domino) with the necessary dependencies, including River-ML and Kafka dependencies.  Install River version 0.1.0 from github as well as confluent-kafka version 1.6.0 via pip.  All required dependencies are included in the provided Docker file: https://quay.io/repository/katieshakman/streaming-online-learning-20200208.
8. Configure models with appropriate settings.  In our benchmarking tests, HoeffdingTreeClassifier model was configured with all defaults except for max_depth: tree.HoeffdingTreeClassifier(max_depth=10) The HalfSpaceTrees model was configured with all defaults except for its seed value: anomaly.HalfSpaceTrees(seed=42)
9. Collect performance metrics for the deployed models. Predictive performance can be incrementally measured using the ROCAUC metric available in River. Metrics can be sent along with inferences (predictions) to the "inferences" Kafka topic created above. They can also be written to a file which is persisted to a blob store or other convenient storage. When analyzing the results on Domino, confluent_compute_statistics.py (included in the repository) can be run to persist the results to a file and generate summary statistics.


## Experiements Results
