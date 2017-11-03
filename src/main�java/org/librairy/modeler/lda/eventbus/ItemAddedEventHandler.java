/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import com.google.common.base.Strings;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.storage.dao.ItemsDao;
import org.librairy.modeler.lda.cache.DelayCache;
import org.librairy.modeler.lda.cache.ModelsCache;
import org.librairy.modeler.lda.dao.CustomItemsDao;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.services.ModelingService;
import org.librairy.modeler.lda.services.ShapeService;
import org.librairy.modeler.lda.tasks.LDAIndividualShapingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class ItemAddedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(ItemAddedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingService modelingService;

    @Autowired
    DelayCache delayCache;

    @Autowired
    ShapeService shapeService;

    @Autowired
    ModelingHelper helper;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Relation.Type.CONTAINS_TO_ITEM, Relation.State.CREATED), "modeler.lda.item.added");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {

        LOG.debug("Item added event received: " + event);
        try{
            Relation relation = event.to(Relation.class);

            String domainUri = relation.getStartUri();
            Long delay = delayCache.getDelay(domainUri);

            if (!modelingService.train(domainUri, delay)){
                String itemUri      = relation.getEndUri();
                shapeService.process(domainUri, itemUri, 5000);
            }

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
