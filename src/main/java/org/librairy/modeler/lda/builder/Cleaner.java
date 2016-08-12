package org.librairy.modeler.lda.builder;

import lombok.Setter;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.storage.UDM;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class Cleaner {

    private static Logger LOG = LoggerFactory.getLogger(Cleaner.class);

    @Autowired @Setter
    UDM udm;

    @Autowired @Setter
    UnifiedColumnRepository columnRepository;


    public void clean(String domainUri){
        // Delete previous Topics
        LOG.info("Deleting existing topics");
        columnRepository.findBy(Relation.Type.EMERGES_IN, "domain", domainUri).forEach(relation -> {
            LOG.info("Deleting topic: " + relation.getStartUri());

            udm.find(Relation.Type.DEALS_WITH_FROM_DOCUMENT).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel ->
                    udm.delete(Relation.Type.DEALS_WITH_FROM_DOCUMENT).byUri(rel.getUri()));

            udm.find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.DEALS_WITH_FROM_ITEM).byUri(rel
                    .getUri()));

            udm.find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.DEALS_WITH_FROM_PART).byUri(rel.getUri
                    ()));

            udm.find(Relation.Type.MENTIONS_FROM_TOPIC).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.MENTIONS_FROM_TOPIC).byUri(rel
                    .getUri()));

            udm.delete(Relation.Type.EMERGES_IN).byUri(relation.getUri());

            udm.delete(Resource.Type.TOPIC).byUri(relation.getStartUri());

        });
        LOG.info("Deleted existing topics");

    }
}
