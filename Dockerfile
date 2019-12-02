#FROM docker.elastic.co/elasticsearch/elasticsearch:5.6.0
FROM elasticsearch:7.4.2
#USER root
RUN yum install git -y
RUN yum install maven -y
RUN yum install unzip -y
RUN mkdir /tmp/es-change-feed-plugin
ADD . /tmp/es-change-feed-plugin/
#USER elasticsearch
