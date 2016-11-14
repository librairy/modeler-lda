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
public class DistributionsDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(DistributionsDao.class);

    public static final String COMBINED_KEY = "combined_key";

    public static final String RESOURCE_URI = "resource_uri";

    public static final String RESOURCE_TYPE = "resource_type";

    public static final String TOPIC_URI = "topic_uri";

    public static final String SCORE = "score";

    public static final String DATE = "date";

    public static final String TABLE = "distributions";

    public DistributionsDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating LDA distributions table for domain: " + domainUri);
        getSession(domainUri).execute("create table if not exists "+table+"(" +
                COMBINED_KEY+" bigint, " +
                RESOURCE_URI+" text, " +
                RESOURCE_TYPE+" text, " +
                TOPIC_URI+" text, " +
                SCORE+" double, " +
                DATE+" text, " +
                "primary key (("+COMBINED_KEY+"), "+RESOURCE_TYPE+","+SCORE+"))" +
                "with clustering order by ("+RESOURCE_TYPE+" ASC, "+SCORE+" DESC);");
        getSession(domainUri).execute("create index on "+table+" ("+RESOURCE_URI+");");
        getSession(domainUri).execute("create index on "+table+" ("+TOPIC_URI+");");
    }


}
