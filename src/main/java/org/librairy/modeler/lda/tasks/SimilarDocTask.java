/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.relations.SimilarToDocuments;
import org.librairy.model.domain.relations.SimilarToItems;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class SimilarDocTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarDocTask.class);

    private final ModelingHelper helper;

    private final Relation relation;


    public SimilarDocTask(Relation relation, ModelingHelper modelingHelper) {
        this.helper = modelingHelper;
        this.relation = relation;
    }


    @Override
    public void run() {
        Optional<Relation> res = helper.getUdm().read(Relation.Type.SIMILAR_TO_ITEMS).byUri(relation
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
        helper.getColumnRepository().findBy(Relation.Type.BUNDLES, "item", item1Uri).forEach( rel -> {
            String document1Uri = rel.getStartUri();

            helper.getColumnRepository().findBy(Relation.Type.BUNDLES, "item", item2Uri).forEach( rel2 -> {
                String document2Uri = rel2.getStartUri();

                SimilarToDocuments similarToDocuments = Relation
                        .newSimilarToDocuments(document1Uri, document2Uri, domainUri);
                similarToDocuments.setWeight(similarItemRel.getWeight());
                similarToDocuments.setDomain(similarItemRel.getDomain());
                helper.getUdm().save(similarToDocuments);
                LOG.debug("Similarity created: " + similarToDocuments);
            });
        });
    }


}
