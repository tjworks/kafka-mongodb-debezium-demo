from python:alpine

USER root
RUN pip install kafka-python
RUN pip install pymongo

