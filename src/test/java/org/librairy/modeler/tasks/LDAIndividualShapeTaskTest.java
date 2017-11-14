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
import org.librairy.boot.storage.UDM;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.LDAIndividualShapingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.TreeSet;

/**
 * Created on 27/06/16:x
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource(
        locations = {"classpath:application.properties"},
        properties = {
//        "librairy.lda.event.value = 60000",
//        "librairy.computing.cluster = local[4]",
//        "librairy.computing.cores = 8"
//        "librairy.computing.cluster = spark://minetur.dia.fi.upm.es:7077",
//        "librairy.computing.cores = 96",
//        "librairy.computing.memory = 82g",
        "librairy.computing.fs = hdfs://minetur.dia.fi.upm.es:9000",
        "librairy.eventbus.host = local"
})
public class LDAIndividualShapeTaskTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDAIndividualShapeTaskTest.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    UDM udm;

    @Test
    public void execute() throws InterruptedException, DataNotFound {

        String domainUri    = "http://librairy.linkeddata.es/resources/domains/jows";
        String documentUri  = "http://librairy.linkeddata.es/resources/items/9_d3ELrTNPCbp"; //from BlueBottle

//        udm.save(Relation.newContains(domainUri, documentUri));

        LDAIndividualShapingTask task = new LDAIndividualShapingTask(domainUri, helper, new TreeSet<>(Arrays.asList(new String[]{documentUri})));

        task.run();

        LOG.info("Sleeping");
        Thread.currentThread().sleep(Integer.MAX_VALUE);

    }

}
