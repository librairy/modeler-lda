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
public class BuildSimilarityTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BuildSimilarityTask.class);

    private final ModelingHelper helper;

    private final String domainUri;

    private final Resource.Type resourceType;

    public BuildSimilarityTask(String domainUri, Resource.Type type, ModelingHelper modelingHelper) {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
        this.resourceType = type;
    }


    @Override
    public void run() {

        LOG.debug("trying to discover topic model-based similarities in domain: " + domainUri);
        helper.getSimilarityBuilder().discover(domainUri, resourceType);

    }


}
