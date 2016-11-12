/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

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
    KeyspaceDao keyspaceDao;

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


    public void initialize(String domainUri){

        keyspaceDao.destroy(domainUri);

        keyspaceDao.initialize(domainUri);

        topicsDao.initialize(domainUri);

        distributionsDao.initialize(domainUri);

        shapesDao.initialize(domainUri);

        annotationsDao.initialize(domainUri);

        similaritiesDao.initialize(domainUri);
    }

}
