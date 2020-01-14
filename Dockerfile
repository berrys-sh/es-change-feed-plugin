FROM elasticsearch:7.3.0
#RUN chown -R elasticsearch:elasticsearch config/jvm.options config/elasticsearch.yml bin/es-docker && \
#    chmod 0750 bin/es-docker

RUN yum install git -y
RUN yum install maven -y
RUN yum install unzip -y
RUN mkdir /usr/tmp/es-change-feed-plugin
ADD . /usr/tmp/es-change-feed-plugin/
WORKDIR /usr/tmp/es-change-feed-plugin/
RUN mvn clean install
#WORKDIR /usr/share/elasticsearch

RUN mkdir -p /usr/share/elasticsearch/plugins/es-change-feed-plugin
RUN cp /usr/tmp/es-change-feed-plugin/target/es-changes-feed-plugin.zip /usr/share/elasticsearch/plugins/es-change-feed-plugin/
WORKDIR /usr/share/elasticsearch/plugins/es-change-feed-plugin
RUN unzip es-changes-feed-plugin.zip
#RUN cp /usr/tmp/es-change-feed-plugin/src/main/resources/plugin-custom.properties /usr/share/elasticsearch/plugins/es-change-feed-plugin/
#RUN cp elasticsearch/*.* .
#RUN rm -rf elasticsearch

WORKDIR /usr/share/elasticsearch
