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
public class SimilaritiesDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(SimilaritiesDao.class);

    public static final String RESOURCE_URI_1 = "resource_uri_1";

    public static final String RESOURCE_TYPE_1 = "resource_type_1";

    public static final String RESOURCE_URI_2 = "resource_uri_2";

    public static final String RESOURCE_TYPE_2 = "resource_type_2";

    public static final String SCORE = "score";

    public static final String DATE = "date";

    public static final String TABLE = "similarities";

    public SimilaritiesDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating LDA "+table+" table for domain: " + domainUri);
        getSession(domainUri).execute("create table if not exists "+table+"(" +
                RESOURCE_URI_1 +" text, " +
                RESOURCE_TYPE_1+" text, " +
                RESOURCE_URI_2 +" text, " +
                RESOURCE_TYPE_2+" text, " +
                SCORE+" double, " +
                DATE+" text, " +
                "primary key ("+RESOURCE_URI_1+", "+SCORE+","+RESOURCE_URI_2+"))" +
                "with clustering order by ("+SCORE+" DESC, "+RESOURCE_URI_2+" ASC"+ ");");

    }


}
