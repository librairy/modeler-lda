FROM tomcat:8-jre8
MAINTAINER Librairy <librairy.framework@gmail.com>

RUN apt-get update && apt-get --fix-missing -y install build-essential python-dev libcurl4-nss-dev libsasl2-dev libsasl2-modules maven libapr1-dev libsvn-dev

ENV LIBRAIRY_HOME /librairy
RUN mkdir $LIBRAIRY_HOME

WORKDIR /usr/local/tomcat

ADD modeler-lda.war /usr/local/tomcat/webapps/modeler-lda.war

ADD waitFor.sh /
RUN chmod +x /waitFor.sh

VOLUME $LIBRAIRY_HOME

EXPOSE 8080
CMD ["catalina.sh", "run"]