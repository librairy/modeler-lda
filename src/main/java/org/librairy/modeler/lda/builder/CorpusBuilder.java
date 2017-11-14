/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class CorpusBuilder {

    private static Logger LOG = LoggerFactory.getLogger(CorpusBuilder.class);

    @Autowired
    ModelingHelper helper;


    public Corpus build(ComputingContext context, String domainUri, List<Resource.Type> types){

        // Reading Uris
        LOG.info("Creating a corpus of "+ types +" from domain: " + domainUri);
        String domainId = URIGenerator.retrieveId(domainUri);

        // Train model
        Corpus corpus = new Corpus(context, domainId, types, helper);
        corpus.loadDomain(domainUri);

        return corpus;

    }


    public Corpus build(ComputingContext context, String domainUri, List<Resource.Type> types, List<String> resources){

        // Reading Uris
        LOG.info("Creating a corpus from domain: " + domainUri + " with: " + resources.size() + " elements");
        String domainId = URIGenerator.retrieveId(domainUri);

        // Train model
        Corpus corpus = new Corpus(context, domainId, types, helper);
        corpus.loadResources(domainUri,resources);

        return corpus;

    }

}
