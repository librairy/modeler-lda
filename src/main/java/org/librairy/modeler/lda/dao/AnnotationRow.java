/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class AnnotationRow implements Serializable{

    private String resource_uri;
    private String resource_type;
    private String type;
    private String value;
    private String date;
    private Double score;
    private Long combined_key;

    public AnnotationRow(String resUri, String resType, String annotationType, String annotationValue, Double score,
                         String date){
        this.combined_key = Long.valueOf((resUri+"-"+annotationType+"-"+annotationType).hashCode());
        this.resource_uri = resUri;
        this.resource_type = resType;
        this.type = annotationType;
        this.value = annotationValue;
        this.date = date;
        this.score = score;
    }
}
