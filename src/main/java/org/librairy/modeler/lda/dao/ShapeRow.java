/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import lombok.Data;
import org.librairy.boot.storage.generator.URIGenerator;

import java.io.Serializable;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class ShapeRow implements Serializable{

    private String uri;
    private Long id;
    private List<Double> vector;
    private String date;
    private String type;
}
