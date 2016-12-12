/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * Created by cbadenes on 11/01/16.
 */
@Configuration("modeler.lda.api")
@ComponentScan({
        "org.librairy.boot.storage.system.column",
        "org.librairy.boot.eventbus",
        "org.librairy.boot.storage.generator",
        "org.librairy.modeler.lda.api",
        "org.librairy.modeler.lda.builder",
        "org.librairy.modeler.lda.cache",
        "org.librairy.modeler.lda.dao",
        "org.librairy.modeler.lda.exceptions",
        "org.librairy.modeler.lda.functions",
        "org.librairy.modeler.lda.helper",
        "org.librairy.modeler.lda.models",
        "org.librairy.modeler.lda.optimizers",
        "org.librairy.modeler.lda.services",
        "org.librairy.modeler.lda.tasks",
        "org.librairy.modeler.lda.utils",
        "org.librairy.computing.cluster",
        "org.librairy.computing.helper",
        "org.librairy.computing.storage",
        "org.librairy.computing.tasks"
})
@PropertySource({"classpath:lda-modeler.properties","classpath:boot.properties","classpath:computing.properties"})
public class ApiConfig {

    //To resolve ${} in @Value
    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
