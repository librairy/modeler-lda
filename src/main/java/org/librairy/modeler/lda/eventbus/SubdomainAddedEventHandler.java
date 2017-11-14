/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.domain.resources.Item;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.modeler.lda.cache.DelayCache;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.services.ModelingService;
import org.librairy.modeler.lda.services.SubdomainShapingService;
import org.librairy.modeler.lda.tasks.LDAIndividualShapingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class SubdomainAddedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(SubdomainAddedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingHelper helper;

    @Autowired
    ModelingService modelingService;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of("subdomain.added"), "modeler.lda.subdomain.added");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {

        LOG.debug("Subdomain added event received: " + event);
        try{
            Relation relation = event.to(Relation.class);

            String domainUri    = relation.getStartUri();
            String subDomainUri = relation.getEndUri();

            if (!modelingService.isPendingModeling(domainUri)){
                // Individually update subdomain
                Optional<String> offset = Optional.empty();
                Integer size = 100;
                Boolean finished = false;
                // documents
                Set<String> uris = new TreeSet<>();
                while(!finished) {
                    List<Item> docs = helper.getDomainsDao().listItems(subDomainUri, size, offset, false);
                    if (docs.isEmpty()) break;
                    uris.addAll(docs.stream().map(i -> i.getUri()).collect(Collectors.toList()));
                    finished = (docs.size() < size);
                    if (!finished) offset = Optional.of(docs.get(size - 1).getUri());
                }
                new LDAIndividualShapingTask(domainUri, helper, uris).run();
            }

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
