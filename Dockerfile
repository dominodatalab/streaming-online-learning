# This Dockerfile has been built as a Docker image that is available at: https://quay.io/repository/katieshakman/streaming-online-learning-20200208
FROM dominodatalab/base:Ubuntu18_DAD_Py3.7_R3.6_20200508

LABEL maintainer="Field Data Science Team at Domino Data Lab <support@dominodatalab.com>"

USER root
RUN pip install git+https://github.com/online-ml/river --upgrade
RUN pip uninstall numpy -y
RUN pip install numpy==1.19.5 # Should be >=1.19
RUN pip install kafka-python==2.0.2
RUN pip install confluent_kafka
