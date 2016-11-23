/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class SimilarityRow implements Serializable{

    private String resource_uri_1;

    private String resource_type_1;

    private String resource_uri_2;

    private String resource_type_2;

    private Double score;

    private String date;

}
