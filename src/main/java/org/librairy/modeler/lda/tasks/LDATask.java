package org.librairy.modeler.lda.tasks;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        // Remove existing topics in domain
        helper.getCleaner().clean(domainUri);

        // Create a new Topic Model
        helper.getOnlineLDABuilder().build(domainUri);

        // Calculate similarities based on the model
        helper.getSimilarityBuilder().update(domainUri);

    }


}
