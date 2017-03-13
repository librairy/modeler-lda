/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component(value = "lda.annotationsDao")
public class AnnotationsDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(AnnotationsDao.class);

    public static final String COMBINED_KEY = "combined_key";

    public static final String RESOURCE_URI = "resource_uri";

    public static final String RESOURCE_TYPE = "resource_type";

    public static final String TYPE = "type";

    public static final String VALUE = "value";

    public static final String SCORE = "score";

    public static final String DATE = "date";

    public static final String TABLE = "annotations";

    public AnnotationsDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating LDA annotations table for domain: " + domainUri);
        try{
            getSession(domainUri).execute("create table if not exists "+table+"(" +
                    COMBINED_KEY +" bigint, " +
                    RESOURCE_URI +" text, " +
                    RESOURCE_TYPE +" text, " +
                    TYPE+" text, " +
                    VALUE+" text, " +
                    SCORE+" double, " +
                    DATE+" text, " +
                    "primary key ("+RESOURCE_URI+", "+SCORE+","+TYPE+","+VALUE+"))" +
                    "with clustering order by ("+SCORE+" DESC, " + TYPE + " ASC, "+VALUE+" ASC);");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }


}
