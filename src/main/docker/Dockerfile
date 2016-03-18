FROM tomcat:8-jre8
MAINTAINER Librairy <librairy.framework@gmail.com>

ENV LIBRAIRY_HOME /librairy
RUN mkdir $LIBRAIRY_HOME

WORKDIR /usr/local/tomcat

ADD modeler-lda.war /usr/local/tomcat/webapps/modeler-lda.war

ENV COLUMN_DB_HOST column-db
ENV COLUMN_DB_PORT 9042

ENV DOCUMENT_DB_HOST document-db
ENV DOCUMENT_DB_PORT 9300

ENV GRAPH_DB_HOST graph-db
ENV GRAPH_DB_PORT 7474

ENV MESSAGE_BROKER_HOST event-bus
ENV MESSAGE_BROKER_PORT 5672

VOLUME $LIBRAIRY_HOME

EXPOSE 8080
CMD ["catalina.sh", "run"]