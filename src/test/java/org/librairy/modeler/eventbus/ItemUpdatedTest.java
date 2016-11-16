/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.eventbus;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.modeler.lda.Config;
import org.librairy.boot.storage.UDM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by cbadenes on 11/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.computing.task.size = 200",
        "librairy.columndb.host = zavijava.dia.fi.upm.es",
        "librairy.documentdb.host = zavijava.dia.fi.upm.es",
        "librairy.graphdb.host = zavijava.dia.fi.upm.es",
        "librairy.eventbus.host = zavijava.dia.fi.upm.es"
//        "librairy.uri = drinventor.eu" //librairy.org
})
public class ItemUpdatedTest {

    private static final Logger LOG = LoggerFactory.getLogger(ItemUpdatedTest.class);

    @Autowired
    EventBus eventBus;

    @Autowired
    UDM udm;

    @Test
    public void itemsUpdated() throws InterruptedException {


        udm.find(Resource.Type.ITEM).all().forEach(res ->{
            eventBus.post(Event.from(res), RoutingKey.of(Resource.Type.ITEM, Resource.State.UPDATED));
        });

    }


}
