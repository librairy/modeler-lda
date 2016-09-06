package org.librairy.modeler.lda.eventbus;

import org.librairy.model.Event;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.modules.BindingKey;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.EventBusSubscriber;
import org.librairy.model.modules.RoutingKey;
import org.librairy.modeler.lda.cache.CacheManager;
import org.librairy.modeler.lda.services.TopicModelingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class ItemBundledEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(ItemBundledEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    TopicModelingService topicModelingService;

    @Autowired
    CacheManager cacheManager;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Relation.Type.BUNDLES, Relation.State.CREATED), "lda-modeler-item");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Document created event received: " + event);
        try{
            Relation relation = event.to(Relation.class);

            // BUNDLES relation
            String domainUri = cacheManager.getDomain(relation.getEndUri());

            topicModelingService.buildModels(domainUri, Resource.Type.ITEM);
        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
