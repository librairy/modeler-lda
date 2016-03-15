package org.librairy.modeler.lda.scheduler;

import org.librairy.model.domain.resources.Analysis;
import org.librairy.model.domain.resources.Resource;
import org.librairy.model.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by cbadenes on 13/01/16.
 */
public abstract class ModelingTask implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(ModelingTask.class);


    protected Analysis newAnalysis(String type, String configuration, String description, String domainUri){
        Analysis analysis = Resource.newAnalysis();
        analysis.setCreationTime(TimeUtils.asISO());
        analysis.setDomain(domainUri);
        analysis.setType(type);
        analysis.setDescription(description);
        analysis.setConfiguration(configuration);
        return analysis;
    }
}
