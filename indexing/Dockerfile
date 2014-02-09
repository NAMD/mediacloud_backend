#
# VERSION 0.1
#
# Solr dockerfile

FROM    debian
MAINTAINER  Flavio Coelho "fccoelho@gmail.com"

RUN mkdir -p /opt
ADD solr-4.6.0.tgz /opt/solr-4.6.0.tgz
RUN tar -C /opt --extract --file /opt/solr-4.6.0.tgz
RUN apt-get update
RUN apt-get --yes install openjdk-6-jdk
EXPOSE 8983
CMD ["/bin/bash", "-c", "cd /opt/solr-4.6.0/example; java -jar start.jar"]
