/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import org.librairy.modeler.lda.api.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class LDAKeyspaceDao {

    private static final Logger LOG = LoggerFactory.getLogger(LDAKeyspaceDao.class);

    @Autowired
    SessionManager sessionManager;

    public void initialize(String domainUri){
        LOG.info("creating a new LDA workspace for domain: " + domainUri);
        sessionManager.getSession().execute("create keyspace if not exists "+sessionManager.getKeyspace(domainUri)+
                " with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
    }

    public void destroy(String domainUri){
        LOG.info("dropping existing LDA workspace for domain: " + domainUri);
        sessionManager.getSession().execute("drop keyspace if exists " + sessionManager.getKeyspace(domainUri)+ ";");
    }

}
