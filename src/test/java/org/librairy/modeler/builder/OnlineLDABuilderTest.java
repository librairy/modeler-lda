package org.librairy.modeler.builder;

import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.resources.Part;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.OnlineLDABuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.modeler.learn = false",
        "librairy.comparator.delay = 1000",
        "librairy.cassandra.contactpoints = 192.168.99.100",
        "librairy.cassandra.port = 5011",
        "librairy.cassandra.keyspace = research",
        "librairy.elasticsearch.contactpoints = 192.168.99.100",
        "librairy.elasticsearch.port = 5021",
        "librairy.neo4j.contactpoints = 192.168.99.100",
        "librairy.neo4j.port = 5030",
        "librairy.eventbus.host = 192.168.99.100",
        "librairy.eventbus.port = 5041",
        "spark.filesystem = file:",
        "librairy.modeler.folder = target",
        "librairy.vocabulary.folder = target"
})
public class OnlineLDABuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(OnlineLDABuilderTest.class);

    @Autowired
    OnlineLDABuilder builder;

    @Autowired
    ModelingHelper modelingHelper;

    @Test
    public void buildOnlineLDA(){

        String domainURI    = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";
        Integer vocabSize   = 10000;

        builder.build(domainURI,vocabSize);

    }
}
