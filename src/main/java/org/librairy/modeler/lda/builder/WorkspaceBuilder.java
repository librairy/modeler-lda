/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.modeler.lda.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class WorkspaceBuilder {

    private static Logger LOG = LoggerFactory.getLogger(WorkspaceBuilder.class);

    @Autowired
    LDAKeyspaceDao keyspaceDao;

    @Autowired
    AnnotationsDao annotationsDao;

    @Autowired
    DistributionsDao distributionsDao;

    @Autowired
    ShapesDao shapesDao;

    @Autowired
    SimilaritiesDao similaritiesDao;

    @Autowired
    TopicsDao topicsDao;

    @Autowired
    CounterDao counterDao;

    @Autowired
    LDACounterDao ldaCounterDao;

    @Autowired
    ComparisonsDao comparisonsDao;


    public void initialize(String domainUri){

        counterDao.reset(domainUri, Resource.Type.TOPIC.route());
        counterDao.reset(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route());

        keyspaceDao.destroy(domainUri);
        keyspaceDao.initialize(domainUri);

        topicsDao.initialize(domainUri);

        distributionsDao.initialize(domainUri);

        shapesDao.initialize(domainUri);

        annotationsDao.initialize(domainUri);

        similaritiesDao.initialize(domainUri);

        ldaCounterDao.initialize(domainUri);

        comparisonsDao.initialize(domainUri);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
