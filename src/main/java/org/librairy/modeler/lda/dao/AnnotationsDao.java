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
public class AnnotationsDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(AnnotationsDao.class);

    public static final String RESOURCE_URI = "resource_uri";

    public static final String TYPE = "type";

    public static final String VALUE = "value";

    public static final String SCORE = "score";

    public static final String DATE = "date";

    public AnnotationsDao() {
        super("annotations");
    }

    public void initialize(String domainUri){
        LOG.info("creating LDA annotations table for domain: " + domainUri);
        getSession(domainUri).execute("create table if not exists "+table+"(" +
                RESOURCE_URI +" text, " +
                TYPE+" text, " +
                VALUE+" text, " +
                SCORE+" double, " +
                DATE+" text, " +
                "primary key (("+RESOURCE_URI+","+TYPE+","+VALUE+"),"+SCORE+"))" +
                "with clustering order by ("+SCORE+" DESC);");
    }


}
