/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.dao;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.dao.ClusterDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

/**
 * Created by cbadenes on 13/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
public class ClusterDaoTest {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterDaoTest.class);

    @Autowired
    ClusterDao clusterDao;

    @Test
    public void getClusters() throws InterruptedException, DataNotFound {

        String domainUri    = "http://librairy.org/domains/user4";
        String documentUri  = "http://librairy.org/items/doc1";
        List<Long> clusters = clusterDao.getClusters(domainUri, documentUri);
        LOG.info("Clusters: " + clusters);
    }
}


