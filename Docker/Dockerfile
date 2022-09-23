# Dockerfile (blueprint for building Image), Image(Template for running containers), Container(package project)
FROM python:3.7

ADD Real_time_twitter_collection.ipynb .

RUN pip install tweepy configparser pymongo

CMD ["python", "./Real_time_twitter_collection.ipynb"]
