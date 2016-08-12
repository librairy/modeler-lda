package org.librairy.modeler.lda.services;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.LDATask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class TopicModelingService {

    private static final Logger LOG = LoggerFactory.getLogger(TopicModelingService.class);

    private ConcurrentHashMap<String,ScheduledFuture<?>> tasks;

    private ThreadPoolTaskScheduler threadpool;


    @Value("${librairy.modeler.delay}")
    protected Long delay;

    @Autowired
    ModelingHelper helper;

    @PostConstruct
    public void setup(){
        this.tasks = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(50);
        this.threadpool.initialize();
    }


    public void buildModels(String domainUri, Resource.Type resourceType){
        LOG.info("Planning a new task for building a topic model (LDA) for the domain: " + domainUri + " and resources: " + resourceType);
        ScheduledFuture<?> task = tasks.get(domainUri+resourceType.name());
        if (task != null) task.cancel(false);
        task = this.threadpool.schedule(new LDATask(domainUri,helper,resourceType), new Date(System.currentTimeMillis() + delay));
        tasks.put(domainUri+resourceType.name(),task);
    }

}
