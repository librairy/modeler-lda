/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.dao;

import es.cbadenes.lab.test.IntegrationTest;
import org.apache.commons.lang.math.IntRange;
import org.assertj.core.util.Strings;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.dao.ClusterDao;
import org.librairy.modeler.lda.dao.ShapeRow;
import org.librairy.modeler.lda.dao.ShapesByTopicDao;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Created by cbadenes on 13/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource({"classpath:boot.properties","classpath:computing.properties", "classpath:application.properties"})

public class ShapesByTopicDaoTest {

    private static final Logger LOG = LoggerFactory.getLogger(ShapesByTopicDaoTest.class);

    @Autowired
    ShapesByTopicDao shapesByTopicDao;

    @Autowired
    ShapesDao shapesDao;

    @Test
    public void initializeTable() throws InterruptedException, DataNotFound {

        String domainUri    = "http://librairy.org/domains/blueBottle";

        shapesByTopicDao.initialize(domainUri);

        // Get shapes and save them into table
        Optional<Long> offset     = Optional.empty();
        Optional<Integer> size    = Optional.of(500);
        AtomicInteger counter     = new AtomicInteger();
        while(true){
            List<ShapeRow> rows = shapesDao.get(domainUri, size, offset);
            counter.set(counter.get()+rows.size());

            for(ShapeRow row : rows){
                shapesByTopicDao.save(domainUri, row);
            }

            LOG.info(counter.get() + " processed");

            if (rows.size() < size.get()){
                break;
            }

            offset = Optional.of(rows.get(size.get()-1).getId());

        }
    }


    public void getSimilarTo(){

        String domainUri    = "http://librairy.org/domains/blueBottle";
        String resourceUri  = "http://librairy.org/items/M57xA96F4Ix";


        try {
            ShapeRow shape = shapesDao.get(domainUri, resourceUri);
            long maxTopic = shapesByTopicDao.getMaxTopic(shape.getVector());


            Optional<Integer> size  = Optional.of(500);
            Optional<String> offset = Optional.empty();

            while(true){

                List<ShapeRow> simResources = shapesByTopicDao.get(domainUri, maxTopic, Optional.of("item"), size, offset);

                if (simResources.size()< size.get()) break;

                offset = Optional.of(simResources.get(simResources.size()-1).getUri());

            }




        } catch (DataNotFound dataNotFound) {
            dataNotFound.printStackTrace();
        }



    }
}


