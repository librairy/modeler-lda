package org.librairy.modeler.lda.tasks;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicDescription;
import org.librairy.modeler.lda.models.TopicModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDATask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDATask.class);

    private final ModelingHelper helper;

    private final String domainUri;

    private final Resource.Type resourceType;

    public LDATask(String domainUri, ModelingHelper modelingHelper, Resource.Type resourceType) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
        this.resourceType = resourceType;
    }


    @Override
    public void run() {

        LOG.info("ready to create a new topic model for domain: " + domainUri);

        // Create corpus
        Corpus corpus = helper.getCorpusBuilder().build(domainUri, Resource.Type.ITEM);

        // Create a new Topic Model
        TopicModel model = helper.getLdaBuilder().build(corpus);

        // Persist it on data model
        Map<String, String> registry = helper.getTopicsBuilder().persist(model);

        // Relate items to topics
        helper.getDealsBuilder().build(corpus,model,registry);

        // Relate parts to topics
        Corpus corpusOfParts = helper.getCorpusBuilder().build(domainUri, Resource.Type.PART);
        corpusOfParts.setCountVectorizerModel(corpus.getCountVectorizerModel());
        helper.getDealsBuilder().build(corpusOfParts,model,registry);

        //TODO Calculate similarities based on the model
//        helper.getSimilarityBuilder().update(domainUri);

    }


}
