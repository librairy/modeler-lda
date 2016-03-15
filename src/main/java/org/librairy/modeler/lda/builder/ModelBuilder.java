package org.librairy.modeler.lda.builder;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 15/01/16.
 */
@Component
public class ModelBuilder {

    private ThreadPoolTaskScheduler threadpool;

    @Value("${librairy.modeler.parallel}")
    Integer parallel;


    @PostConstruct
    public void setup(){
        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(parallel);
        this.threadpool.initialize();
    }

    public void execute(Runnable modelingTask){
        // TODO parallel
//        this.threadpool.execute(modelingTask);
        modelingTask.run();
    }

}
