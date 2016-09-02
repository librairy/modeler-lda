package org.librairy.modeler.lda;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * Created by cbadenes on 11/01/16.
 */
@Configuration("modeler.lda")
@ComponentScan({"org.librairy"})
@PropertySource({"classpath:lda-modeler.properties","classpath:boot.properties","classpath:computing.properties"})
public class Config {

    //To resolve ${} in @Value
    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
