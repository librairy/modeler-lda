# LDA Modeler [![Release Status](https://travis-ci.org/librairy/modeler-lda.svg?branch=master)](https://travis-ci.org/librairy/modeler-lda) [![Dev Status](https://travis-ci.org/librairy/modeler-lda.svg?branch=develop)](https://travis-ci.org/librairy/modeler-lda) [![Doc](https://raw.githubusercontent.com/librairy/resources/master/figures/interface.png)](https://rawgit.com/librairy/modeler-lda/doc/report/index.html)

Build a Topic Model based on Latent Dirichlet Allocation (LDA) for a given domain

## Get Started!

A prerequisite to consider is to have installed [Docker-Compose](https://docs.docker.com/compose/) in your system.

You can run this service in a isolated way (see *Distibuted Deployment* section) or as extension of the [explorer](https://github.com/librairy/explorer).
In that case, add the following services to the existing `docker-compose.yml` file:

```yml
modelerLDA:
  container_name: modelerLDA
  image: librairy/modeler-lda
  links:
      - column-db
      - document-db
      - graph-db
      - event-bus
```

and then, deploy it by typing:

```sh
$ docker-compose up
```
That's all!! **librairy LDA modeler** should be run in your system now along with **librairy explorer**.

## Distributed Deployment

Instead of deploy all containers as a whole, you can deploy each of them independently. It is useful to run the service in a distributed way deployed in several host-machines.

- **LDA-Modeler**:

    ```sh
    $ docker run -it --rm --name modelerLDA librairy/modeler-lda
    ```

Remember that by using the flags: `-it --rm`, the services runs in foreground mode. Instead, you can deploy it in background mode as a domain service by using: `-d --restart=always`
