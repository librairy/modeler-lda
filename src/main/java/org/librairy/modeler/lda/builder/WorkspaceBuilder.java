package org.librairy.modeler.lda.builder;

import es.upm.oeg.epnoi.matching.metrics.domain.entity.ConceptualResource;
import es.upm.oeg.epnoi.matching.metrics.domain.entity.RegularResource;
import es.upm.oeg.epnoi.matching.metrics.domain.space.ConceptsSpace;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class WorkspaceBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(WorkspaceBuilder.class);

    @Autowired
    CRBuilder crBuilder;

    public ConceptsSpace from(String id, JavaRDD<RegularResource> regularResources){

        LOG.info("Number of Regular Resources: " + regularResources.count() + " in domain: '" + id + "'");

        // Convert Regular Resources to Conceptual Resources
        JavaRDD<ConceptualResource> conceptualResources = regularResources.map(crBuilder);
        LOG.info("Number of Conceptual Resources: " + regularResources.count() + " in domain: '" + id + "'");

        // Create the Concepts Space
        return new ConceptsSpace(conceptualResources.rdd());
    }
}
