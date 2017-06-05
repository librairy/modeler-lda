/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class CustomPartsDao extends AbstractDao{

    public static final Logger LOG = LoggerFactory.getLogger(CustomPartsDao.class);

    public static final String TABLE = "parts";

    public CustomPartsDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){}


    public String getTokens(String domainUri, String partUri){
        String domainId = URIGenerator.retrieveId(domainUri);
        String query = "select tokens from "+TABLE+" where uri='"+partUri+"';";

        try{;
            ResultSet result = sessionManager.getDomainSession(domainId).execute(query);
            Row row = result.one();

            if ((row == null)) return "";

            return row.getString(0);
        } catch (InvalidQueryException e){
            LOG.warn("Error on query: " + e.getMessage());
            return "";
        }
    }




}
