/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;

import java.io.Serializable;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Data
public class Text implements Serializable{

    private String id;

    private String content;

    public Text(){
    }

    public Text(String id, String content){
        this.id = id;
        this.content = content;
    }
}
