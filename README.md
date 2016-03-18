# Librairy Modeler LDA - Files [![Build Status](https://travis-ci.org/librairy/modeler-lda.svg?branch=develop)](https://travis-ci.org/librairy/modeler-lda)

Build a Topic Model by LDA

## Get Start!

The only prerequisite to consider is to install [Docker](https://www.docker.com/) in your system.

Then, run `docker-compose up` by using this file `docker-compose.yml`:  

```yml
modelerLDA:
  container_name: modelerLDA
  image: librairy/modeler-lda
  restart: always
  links:
      - column-db
      - document-db
      - graph-db
      - event-bus
```

That's all!! **librairy comparator cos** should be run in your system now!

Instead of deploy all containers as a whole, you can deploy each of them independently. It is useful to run the service distributed in several host-machines.

## FTP Server

```sh
docker run -it --rm --name modelerLDA librairy/modeler-lda
```

Remember that, by using the flags: `-it --rm`, the services runs in foreground mode, if you want to deploy it in background mode,  or even as a domain service, you should use: `-d --restart=always`
