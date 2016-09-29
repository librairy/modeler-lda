/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.exceptions;

/**
 * Created on 07/09/16:
 *
 * @author cbadenes
 */
public class EmptyResultException extends RuntimeException {

    public EmptyResultException(String msg){
        super(msg);
    }
}
