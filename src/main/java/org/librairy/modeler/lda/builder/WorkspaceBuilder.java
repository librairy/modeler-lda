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
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.models.ComputingKey;
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

    @Autowired
    TagsDao tagsDao;

    @Autowired
    ClusterDao clusterDao;

    @Autowired
    StorageHelper storageHelper;

    public void initialize(String domainUri) throws InterruptedException {

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

        tagsDao.initialize(domainUri);

        clusterDao.initialize(domainUri);

        Thread.sleep(1000);
    }

    public void destroy(String domainUri){
        keyspaceDao.destroy(domainUri);


        // delete models from filesystem
        try {
            String id = URIGenerator.retrieveId(domainUri);
            String absoluteModelPath = storageHelper.absolutePath(storageHelper.path(id, "lda"));
            storageHelper.deleteIfExists(absoluteModelPath);
        }catch (Exception e){
            LOG.error("Error deleting model", e);
        }

    }

}
