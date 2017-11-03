/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.tasks;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.builder.WorkspaceBuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.LDATrainingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource({"classpath:boot.properties","classpath:computing.properties", "classpath:application.properties"})
public class LDATrainingTaskTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDATrainingTaskTest.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    WorkspaceBuilder workspaceBuilder;

    @Test
    public void execute() throws InterruptedException, DataNotFound {
        String domainUri = URIGenerator.fromId(Resource.Type.DOMAIN, "blueBottle");

        LDATrainingTask task = new LDATrainingTask(domainUri, helper);

        task.run();

        LOG.info("Sleeping");
        Thread.currentThread().sleep(Integer.MAX_VALUE);

    }
}
