package org.librairy.modeler.lda.services;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.builder.TopicsBuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.LDACreationTask;
import org.librairy.modeler.lda.tasks.LDAModelingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class ModelingService {

    private static final Logger LOG = LoggerFactory.getLogger(ModelingService.class);

    private ConcurrentHashMap<String,ScheduledFuture<?>> buildingTasks;
    private ConcurrentHashMap<String,ScheduledFuture<?>> modelingTasks;

    private ThreadPoolTaskScheduler threadpool;

    @Autowired
    ModelingHelper helper;

    @Autowired
    TopicsBuilder topicsBuilder;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    @PostConstruct
    public void setup(){
        this.buildingTasks = new ConcurrentHashMap<>();
        this.modelingTasks = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(50);
        this.threadpool.initialize();
    }


    public void train(String domainUri, long delay){
        LOG.info("Scheduled creation of a new topic model (LDA) for the domain: " + domainUri + " at " + timeFormatter
                .format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = buildingTasks.get(domainUri);
        if (task != null) task.cancel(false);
        task = this.threadpool.schedule(new LDACreationTask(domainUri, helper), new Date(System.currentTimeMillis() + delay));
        buildingTasks.put(domainUri,task);

        LOG.info("Cleaning previous topic models if exist");
        topicsBuilder.delete(domainUri);
    }


    public void inferDistributions(String domainUri, Resource.Type resourceType, long delay){
        LOG.debug("Scheduled discover topic distributions from an existing topic model in domain: " +
                domainUri + " at " + timeFormatter.format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = modelingTasks.get(domainUri+resourceType.name());
        if (task != null) task.cancel(false);
        task = this.threadpool.schedule(new LDAModelingTask(domainUri,helper), new Date(System.currentTimeMillis() + delay));
        modelingTasks.put(domainUri+resourceType.name(),task);
    }

}
