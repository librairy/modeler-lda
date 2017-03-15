/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.librairy.boot.eventbus.EventMessage;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.dao.DomainsDao;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.services.ParallelExecutorService;
import org.librairy.modeler.lda.tasks.LDASimilarityGraphTask;
import org.librairy.modeler.lda.tasks.LDASimilarityTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class LdaSimilarityGraphCreatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(LdaSimilarityGraphCreatedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingHelper helper;

    private ParallelExecutorService executor;
    private ObjectMapper jsonMapper;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(LDASimilarityGraphTask.ROUTING_KEY_ID), "modeler.lda.similarity.graph.created");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
        executor    = new ParallelExecutorService();
        jsonMapper   = new ObjectMapper();
    }

    @Override
    public void handle(Event event) {
        LOG.info("lda similarity graph created event received: " + event);
        try{
            String domainUri = event.to(String.class);

            Domain domain = helper.getDomainsDao().get(domainUri);

            LOG.info("Domain '"+domainUri+"' updated with a new LDA Model!!");
            eventBus.post(Event.from(domain), RoutingKey.of(Resource.Type.DOMAIN, Resource.State.UPDATED));

            EventMessage eventMessage = new EventMessage();
            eventMessage.setName(domain.getName());
            eventMessage.setId(URIGenerator.retrieveId(domainUri));
            eventMessage.setTime(TimeUtils.asISO());

            String jsonMsg = jsonMapper.writeValueAsString(eventMessage);
            eventBus.publish(jsonMsg, "domains.analyzed");

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling similarity-graph creation in domain: " + event, e);
        }
    }
}
