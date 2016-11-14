/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ShapesDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(ShapesDao.class);

    public static final String RESOURCE_URI = "uri";

    public static final String RESOURCE_ID = "id";

    public static final String VECTOR = "vector";

    public static final String DATE = "date";

    public static final String TABLE = "shapes";

    public ShapesDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating LDA shapes table for domain: " + domainUri);
        getSession(domainUri).execute("create table if not exists "+table+"(" +
                RESOURCE_ID +" bigint, " +
                RESOURCE_URI +" text, " +
                VECTOR+" list<double>, " +
                DATE+" text, " +
                "primary key ("+RESOURCE_ID+"));");
        getSession(domainUri).execute("create index on "+table+" ("+RESOURCE_URI+");");

    }


}