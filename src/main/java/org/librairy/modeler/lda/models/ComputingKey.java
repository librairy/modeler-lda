/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;
import org.librairy.computing.cluster.ComputingContext;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class ComputingKey {

    ComputingContext context;

    String id;

    public ComputingKey(ComputingContext context , String id){
        this.context = context;
        this.id = id;
    }
}
