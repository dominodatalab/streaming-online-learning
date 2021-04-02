# Online Learning Deployment for Streaming Applications

This repository is the official implementation of the paper Online Learning Deployment for Streaming Applications in the Banking Sector (Barry, Montiel, Bifet, Chiky, Shakman, Manchev, Wadkar, El Baroudi, Tran, KDD 2021). The ressources can be used to set up and deploy instances of online machine learning models, to generate predictions and update the model weights on streaming data.  

> **Problem Statement** Our goal is to propose a solution that can accelare the industrialization and operationalization of online machine learning models for production and application that requires handling streaming constraints. For example the potential use cases can be anomaly and fraud detection for time-evolving data streams or real-time classification of user activities or IT or logs events.

## Technologies and Packages used : RIVER Machine Learning & Domino Platform

> [River](https://github.com/online-ml/river) is an open-source online machine learning library written in Python which main focus is **instance-incremental
learning**, meaning that every component (estimators, transformers, performance metrics, etc.) is designed to be updated one sample at a time. We used River to continuously train and update online learning model from last data streams. 
> [KAFKA](https://kafka.apache.org/) is 
a state of the art open-source distributed
event streaming platform and we used a managed hosted Kafka ([confluent](https://www.confluent.io/). We used it as a data streams generator.

> The [Domino Platform](https://www.dominodatalab.com/) platform is implemented on top of Kubernetes, where
it spins up containers on demand for running user workloads. The containers are based on Docker images, which are fully customizable. We used Domino to host the models and run scalability tests on hig velocity data generated as streams. 

<img width="484" alt="technologies_used_river_domino" src="https://user-images.githubusercontent.com/27995832/113413633-6655d280-93bb-11eb-9f0d-d9674024d465.PNG">

## Files Descriptions

The files in the reposirory are intended to achieve some data processing steps in the streams learning pipeline.

* Set up streams generators using Kafka producer and consumers to process data as streams, create requires features for model input and output. Files #confluent_producer.py 
* Run model training and serving on multiples instances of Domino instances to bechnmark scalability and compute results metrics
* Model run to continuous train and update model weights

## Model Deployment : step to step Online Learning Pipeline

Here we list the high-level steps to set up, deploy, run, and evaluate online learning experiments: 

1. Deploy infrastructure to host experiments (for our purposes this was done on the AWS cloud).  
2. Set up Kafka to generate data streams (this can be done with a managed Kafka service such as Confluent Kafka, although any Kafka solution should work). You will need both topics of data to read from (we expect 8 partitions of the data stream) as well as topics where you can write results. 
 
4. [Optional] Set up or gain access to Domino environment (alternatives can be set up using other solutions).  
5. Connect your compute/model instances to your Kafka cluster.  An example configuration is shown in https://github.com/dominodatalab/streaming-online-learning/blob/main/src/hostedkafka/KafkaConsumer.py. Model instances will pull from a stream on designated topics and write back results on separate topics.
6. Set up the Kafka producer stream on the Kafka end.  This is the stream from which the model instances will pull data for inference and learning.  A producer configuration is demonstrated in https://github.com/dominodatalab/streaming-online-learning/blob/main/src/hostedkafka/confluent_producer.py. 
7. Utilize an appropriate Docker image or virtual environment (or a compute environment if using Domino) with the necessary dependencies, including River-ML and Kafka dependencies.  Install River version 0.1.0 from github as well as confluent-kafka version 1.6.0 via pip.  All required dependencies are included in the provided Docker file: https://quay.io/repository/katieshakman/streaming-online-learning-20200208.
8. Configure models with appropriate settings.  In our benchmarking tests, HoeffdingTreeClassifier model was configured with all defaults except for max_depth: tree.HoeffdingTreeClassifier(max_depth=10) The HalfSpaceTrees model was configured with all defaults except for its seed value: anomaly.HalfSpaceTrees(seed=42)
9. Collect performance metrics for the deployed models. Predictive performance can be incrementally measured using the ROCAUC metric available in River. Metrics can be sent along with inferences (predictions) to the "inferences" Kafka topic created above. They can also be written to a file which is persisted to a blob store or other convenient storage. When analyzing the results on Domino, confluent_compute_statistics.py (included in the repository) can be run to persist the results to a file and generate summary statistics.

## Experiments and Results

 We set up **onlinelearning  models** (supervised HoeffdingTree Classifier Trees [[1]](#2) and unsupervised anomaly dtetector HalfSpaces Trees [[2]](#2)) to incrementally learn and update from streams events. The **datasets** used are publiclyavailable on River : [Credit cards transactions in September 2013 by european
    cardholders](https://github.com/online-ml/river/blob/master/river/datasets/credit_card.py) and [Electricity prices in New South Wales](https://github.com/online-ml/river/blob/master/river/datasets/elec2.py) The pipeline was hosted on AWS
Cloud using the Domino Data Science platform connected to a managed Kafka to process streams data.
The workflow of experiments set up is below and detail are provided in the paper.

<img width="518" alt="pipeline_experiments_kafka_domino" src="https://user-images.githubusercontent.com/27995832/113413618-5c33d400-93bb-11eb-88e9-725aaed545f6.PNG">

A series of experiments was conducted with the main objective
being the functional verification of the proposed streaming architecture in the paper and scalability
exercise. The results table can be found below :

<img width="744" alt="results_experiments" src="https://user-images.githubusercontent.com/27995832/113413601-53430280-93bb-11eb-88fe-06556b192709.PNG">


We demonstrate that the proposed system can successfully ingest and process high-velocity streaming data and that online learning models can be horizontally scaled. 

### References
<a id="1">[1]</a> 
Dijkstra, E. W. (1968). 
Go to statement considered harmful. 
Communications of the ACM, 11(3), 147-148.
