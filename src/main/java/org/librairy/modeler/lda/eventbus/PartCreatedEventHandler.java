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
import org.librairy.modeler.lda.services.ModelingService;
import org.librairy.modeler.lda.services.SimilarityService;
import org.librairy.storage.UDM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class PartCreatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(PartCreatedEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingService modelingService;

    @Autowired
    SimilarityService similarityService;

    @Autowired
    UDM udm;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Relation.Type.DESCRIBES, Relation.State.CREATED),
                "lda-modeler-part-created");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Part created event received: " + event);
        try{

            Relation relation = event.to(Relation.class);

            // Infer topic distribution
            String partUri  = relation.getStartUri();
            String itemUri  = relation.getEndUri();


            // BUNDLES relation
            udm.find(Resource.Type.DOMAIN).from(Resource.Type.ITEM, itemUri).forEach( domain ->{

                // Schedule clean of existing similarity relations
                similarityService.clean(domain.getUri(), 5000);

                // infer relations
                modelingService.inferDistributions(domain.getUri(), Resource.Type.PART, 10000);


            } );


        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
