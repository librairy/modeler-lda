/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api;

import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.api.model.ScoredResource;
import org.librairy.modeler.lda.api.model.ScoredTag;
import org.librairy.modeler.lda.api.model.ScoredTopic;
import org.librairy.modeler.lda.dao.SessionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class LDAModelerAPI {

    @Autowired
    SessionManager sessionManager;


    public List<ScoredResource> getMostRelevantResources(String topicUri, Criteria criteria){
        return null;
    }


    public List<ScoredTopic> getTopicsDistribution(String resourceUri, Criteria criteria){
        return null;
    }

    public List<ScoredTag> getTags(String resourceUri, Criteria criteria){
        return null;
    }

    public List<ScoredResource> getSimilarResources(String resourceUri, Criteria criteria){
        return null;
    }

    public List<ScoredResource> getDiscoveryPath(String startUri, String endUri, Criteria criteria){
        return null;
    }


}
