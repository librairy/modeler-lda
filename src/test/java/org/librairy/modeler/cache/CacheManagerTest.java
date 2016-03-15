package org.librairy.modeler.cache;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.cache.CacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by cbadenes on 15/03/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.cassandra.contactpoints = wiig.dia.fi.upm.es",
        "librairy.cassandra.port = 5011",
        "librairy.cassandra.keyspace = research",
        "librairy.elasticsearch.contactpoints = wiig.dia.fi.upm.es",
        "librairy.elasticsearch.port = 5021",
        "librairy.neo4j.contactpoints = wiig.dia.fi.upm.es",
        "librairy.neo4j.port = 5030",
        "librairy.eventbus.host = wiig.dia.fi.upm.es"
})
public class CacheManagerTest {


    @Autowired
    CacheManager cacheManager;


    @Test
    public void insertAndRead(){


        String itemUri = "http://drinventor.eu/items/df104d2da543dd2a8d4fd67d6c52eb8d";

        String domainUri = cacheManager.getDomain(itemUri);

        Assert.assertEquals("http://drinventor.eu/domains/20160315163752388-5d5b39b9f122694277e7809ea6d6a8bb",domainUri);

        domainUri = cacheManager.getDomain(itemUri);

        Assert.assertEquals("http://drinventor.eu/domains/20160315163752388-5d5b39b9f122694277e7809ea6d6a8bb",domainUri);
    }

}
