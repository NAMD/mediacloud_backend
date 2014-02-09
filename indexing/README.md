# Mongodb Configuration

It is recommended that mongodb is configured as a replica-set due to limitaions in mongo-connector.

# Solr Configuration

schema.xml must be copied to "/etc/solr/conf/schema.xml" on the solr server.

Solr is currently configured to run as a docker container. The container can be started with the following command:

sudo docker run -p 4001:8983 -d fccoelho/solr4:4.6.0
