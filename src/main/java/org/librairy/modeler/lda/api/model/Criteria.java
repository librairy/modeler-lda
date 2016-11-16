/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api.model;

import lombok.Data;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.generator.URIGenerator;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class Criteria {

    String domainUri = URIGenerator.fromId(Resource.Type.DOMAIN,"default");

    Integer max = 10;

    Double threshold = 0.5;

    // todo
//    Resource.Type types = Resource.Type.ANY;

}
