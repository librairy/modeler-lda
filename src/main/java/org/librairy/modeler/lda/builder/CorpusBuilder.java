package org.librairy.modeler.lda.builder;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

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

    @Value("${librairy.modeler.maxiterations}")
    Integer maxIterations;

    @Value("${librairy.vocabulary.size}")
    Integer vocabularySize;

    public Corpus build(String domainUri, Resource.Type type){

        // Reading Uris
        LOG.info("Creating a corpus of "+ type.route() +" from domain: " + domainUri);
        List<String> uris = udm.find(type)
                .from(Resource.Type.DOMAIN, domainUri)
                .parallelStream()
                .map(res -> res.getUri())
                .collect(Collectors.toList());

        if (uris.isEmpty()) throw new RuntimeException("No "+type.route()+" found in domain: " + domainUri);

        String domainId = URIGenerator.retrieveId(domainUri);

        // Train model
        Corpus corpus = build(domainId,uris);

        return corpus;

    }

    public Corpus build(String id, List<String> uris){

        // Initialize a corpus based on uris
        return new Corpus(id, uris, vocabularySize, helper);
    }

}
