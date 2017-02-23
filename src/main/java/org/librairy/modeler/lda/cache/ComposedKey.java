/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.cache;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
@EqualsAndHashCode
public class ComposedKey implements Serializable {

    String domainUri;
    String resourceUri;
}
