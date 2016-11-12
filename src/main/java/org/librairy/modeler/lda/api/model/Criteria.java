/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api.model;

import lombok.Data;
import org.librairy.model.domain.resources.Resource;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class Criteria {

    Integer max;

    Double threshold;

    Resource.Type types;
}
