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
public class TopicsDao extends AbstractDao{

    public static final Logger LOG = LoggerFactory.getLogger(TopicsDao.class);

    public static final String URI = "uri";

    public static final String ID = "id";

    public static final String DESCRIPTION = "description";

    public static final String ELEMENTS = "elements";

    public static final String SCORES = "scores";

    public static final String DATE = "date";

    public static final String TABLE = "topics";

    public TopicsDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating a LDA topics table for domain: " + domainUri);
        getSession(domainUri).execute("create table if not exists "+table+"(" +
                URI+" text, " +
                ID+" bigint, " +
                DESCRIPTION+" text, " +
                ELEMENTS+" list<text>, " +
                SCORES+" list<double>, " +
                DATE+" text, " +
                "primary key ("+URI+","+ID+"));");
    }

}
