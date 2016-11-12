/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.builder;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
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
    UDM udm;

    @Autowired
    ModelingHelper helper;


    public Corpus build(String domainUri, List<Resource.Type> types){

        // Reading Uris
        LOG.info("Creating a corpus of "+ types +" from domain: " + domainUri);
        String domainId = URIGenerator.retrieveId(domainUri);

        // Train model
        Corpus corpus = new Corpus(domainId, types, helper);
        corpus.loadDomain(domainUri);

        return corpus;

    }

//    public Corpus build(String id, List<String> uris){
//
//        // Reading Uris
//        LOG.info("Creating the corpus '" + id + "' from "+ uris.size() +" uris");
//
//        // Train model
//        Corpus corpus = new Corpus(id, Arrays.asList(new Resource.Type[]{Resource.Type.ANY}), helper);
//        corpus.loadResources(uris);
//
//        return corpus;
//
//    }

}
