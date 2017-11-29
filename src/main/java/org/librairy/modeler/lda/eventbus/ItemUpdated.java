/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.storage.dao.DomainsDao;
import org.librairy.modeler.lda.cache.DelayCache;
import org.librairy.modeler.lda.cache.DomainCache;
import org.librairy.modeler.lda.cache.ModelsCache;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.services.ModelingService;
import org.librairy.modeler.lda.services.ShapeService;
import org.librairy.modeler.lda.tasks.LDAIndividualShapingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ItemUpdated implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(ItemUpdated.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    DomainCache domainCache;

    @Autowired
    ModelingService modelingService;

    @Autowired
    ShapeService shapeService;

    @Autowired
    DelayCache delayCache;

    @Autowired
    ModelingHelper helper;

    @Autowired
    ModelsCache modelsCache;

    @Autowired
    DomainsDao domainsDao;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Resource.Type.ITEM, Resource.State.UPDATED ), "modeler.lda.item.updated");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Item updated: " + event);
        try{
            Resource resource = event.to(Resource.class);

            // update domains containing item
            List<Domain> domains = domainCache.getDomainsFrom(resource.getUri());
            domains
                    .forEach(domain ->{
                            domainsDao.updateDomainTokens(domain.getUri(), resource.getUri(), null);
                            Long delay = delayCache.getDelay(domain.getUri());
                            if (!modelingService.train(domain.getUri(),delay)){
                                // Individually update item
                                String itemUri      = resource.getUri();
                                String domainUri    = domain.getUri();

                                shapeService.process(domainUri, itemUri, delay);
                            }
                        }
                    );

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}