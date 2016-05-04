package org.librairy.modeler.lda.builder;

import es.upm.oeg.epnoi.matching.metrics.domain.entity.ConceptualResource;
import es.upm.oeg.epnoi.matching.metrics.domain.entity.RegularResource;
import org.apache.spark.api.java.function.Function;
import org.springframework.stereotype.Component;

/**
 * Created on 02/05/16:
 *
 * @author cbadenes
 */
@Component
public class CRBuilder implements Function<RegularResource, ConceptualResource> {

    @Override
    public ConceptualResource call(RegularResource regularResource) throws Exception {

        return new ConceptualResource(regularResource);
    }
}