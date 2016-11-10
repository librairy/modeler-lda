/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class SimilarToRow {

    private String uri;
    private String domain;
    private String creationtime;
    private String starturi;
    private String enduri;
    private Double weight;
    private long id;
}
