/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import org.librairy.boot.storage.dao.DomainsDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class CustomItemsDao {

    public static final Logger LOG = LoggerFactory.getLogger(CustomItemsDao.class);

    @Autowired
    DomainsDao domainsDao;

    public String getTokens(String domainUri, String itemUri){

        Optional<String> tokens = domainsDao.getDomainTokens(domainUri, itemUri);
        if (!tokens.isPresent()) return "";
        return tokens.get();
    }




}
