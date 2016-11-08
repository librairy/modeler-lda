/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import org.librairy.model.Event;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.modules.BindingKey;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.EventBusSubscriber;
import org.librairy.model.modules.RoutingKey;
import org.librairy.modeler.lda.cache.TopicCache;
import org.librairy.modeler.lda.services.SimilarityService;
import org.librairy.storage.UDM;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
//@Component
public class DealsWithItemEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(DealsWithItemEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    SimilarityService similarityService;

    @Autowired
    UnifiedColumnRepository columnRepository;

    @Autowired
    UDM udm;

    @Autowired
    TopicCache topicCache;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Relation.Type.DEALS_WITH_FROM_ITEM, Relation.State.CREATED),
                "modeler.lda.dealsWithFromItem.created");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Item described by Topic Model event received: " + event);
        try{
            // DEALS_WITH(ITEM) relation
            Relation relation   = event.to(Relation.class);
            String itemUri      = relation.getStartUri();
            String topicUri     = relation.getEndUri();

            // Inference DEALS_WITH(DOCUMENT) from Item
//            columnRepository.findBy(Relation.Type.BUNDLES, "item", itemUri).forEach( rel -> {
//                String documentUri = rel.getStartUri();
//                DealsWithFromDocument dealsWith = Relation
//                        .newDealsWithFromDocument(documentUri, relation.getEndUri());
//                dealsWith.setWeight(relation.getWeight());
//                udm.save(dealsWith);
//                LOG.debug("Topic distribution created: " + dealsWith);
//            });

            // Schedule discover similarities
            String domainUri = this.topicCache.getDomainFromTopic(topicUri);
            similarityService.discover(domainUri, Resource.Type.ITEM,10000);
            
        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
