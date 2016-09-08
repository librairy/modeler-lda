package org.librairy.modeler.lda.eventbus;

import org.librairy.model.Event;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.SimilarToDocuments;
import org.librairy.model.domain.relations.SimilarToItems;
import org.librairy.model.modules.BindingKey;
import org.librairy.model.modules.EventBus;
import org.librairy.model.modules.EventBusSubscriber;
import org.librairy.model.modules.RoutingKey;
import org.librairy.modeler.lda.services.ModelingService;
import org.librairy.storage.UDM;
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
    ModelingService modelingService;

    @Autowired
    UnifiedColumnRepository columnRepository;

    @Autowired
    UDM udm;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of(Relation.Type.SIMILAR_TO_ITEMS, Relation.State.CREATED),
                "lda-modeler-similar-to-item");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }

    @Override
    public void handle(Event event) {
        LOG.debug("Item described by Topic Model event received: " + event);
        try{
            // SIMILAR_TO(ITEM) relation
            Relation relation   = event.to(Relation.class);

            Optional<Relation> res = udm.read(Relation.Type.SIMILAR_TO_ITEMS).byUri(relation
                    .getUri());

            if (!res.isPresent()){
                LOG.warn("SIMILAR_To relation event received but uri not found: " + relation);
                return;
            }

            SimilarToItems similarItemRel = res.get().asSimilarToItems();
            String item1Uri      = similarItemRel.getStartUri();
            String item2Uri      = similarItemRel.getEndUri();
            String domainUri     = similarItemRel.getDomain();

            // inference SIMILAR_TO(DOCUMENT) from Item
            columnRepository.findBy(Relation.Type.BUNDLES, "item", item1Uri).forEach( rel -> {
                String document1Uri = rel.getStartUri();

                columnRepository.findBy(Relation.Type.BUNDLES, "item", item2Uri).forEach( rel2 -> {
                    String document2Uri = rel2.getStartUri();

                    SimilarToDocuments similarToDocuments = Relation
                            .newSimilarToDocuments(document1Uri, document2Uri, domainUri);
                    similarToDocuments.setWeight(similarItemRel.getWeight());
                    similarToDocuments.setDomain(similarItemRel.getDomain());
                    udm.save(similarToDocuments);
                    LOG.debug("Similarity created: " + similarToDocuments);
                });
            });
        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling a new topic model for Items from domain: " + event, e);
        }
    }
}
