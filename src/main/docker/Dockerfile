FROM librairy/tomcat:1.0
MAINTAINER Librairy <librairy.framework@gmail.com>

ENV LIBRAIRY_HOME /librairy
RUN mkdir $LIBRAIRY_HOME

WORKDIR /usr/local/tomcat

ADD modeler-lda.war /usr/local/tomcat/webapps/modeler-lda.war

ADD waitFor.sh /
RUN chmod +x /waitFor.sh

VOLUME $LIBRAIRY_HOME

EXPOSE 8080
CMD ["catalina.sh", "run"]