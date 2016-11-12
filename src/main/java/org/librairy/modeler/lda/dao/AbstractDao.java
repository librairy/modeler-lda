/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public abstract class AbstractDao {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDao.class);

    @Autowired
    private SessionManager sessionManager;

    protected final String table;

    public AbstractDao(String table){
        this.table = table;
    }

    public abstract void initialize(String domainUri);

    public void destroy(String domainUri){
        LOG.info("dropping existing LDA "+table+" table for domain: " + domainUri);
        sessionManager.getSession(domainUri).execute("truncate "+table+";");
    }

    public Session getSession(String domainUri){
        return sessionManager.getSession(domainUri);
    }

}
