/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.eventbus;

import org.librairy.model.Event;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.SimilarToDocuments;
import org.librairy.model.domain.relations.SimilarToItems;
import org.librairy.model.modules.BindingKey;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.EventBusSubscriber;
import org.librairy.model.modules.RoutingKey;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.services.ModelingService;
import org.librairy.modeler.lda.tasks.SimilarDocTask;
import org.librairy.storage.UDM;
import org.librairy.storage.executor.ParallelExecutor;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Optional;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class SimilarToItemEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarToItemEventHandler.class);

    @Autowired
    protected EventBus eventBus;

    @Autowired
    ModelingHelper helper;

    ParallelExecutor executor;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Relation.Type.SIMILAR_TO_ITEMS, Relation.State.CREATED),
                "lda-modeler-similar-to-item");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        executor = new ParallelExecutor();
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Item described by Topic Model event received: " + event);
        try{
            // SIMILAR_TO(ITEM) relation
            Relation relation   = event.to(Relation.class);

            executor.execute(new SimilarDocTask(relation,helper));
        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
