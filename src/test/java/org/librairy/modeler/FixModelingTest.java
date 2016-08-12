package org.librairy.modeler;

import com.google.common.base.Strings;
import es.cbadenes.lab.test.IntegrationTest;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.topic.TopicModeler;
import org.librairy.storage.UDM;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by cbadenes on 13/01/16.
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
        "librairy.modeler.folder = target/models",
})
public class FixModelingTest {

    private static final Logger LOG = LoggerFactory.getLogger(FixModelingTest.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    UDM udm;


    @Test
    public void topicModel() throws InterruptedException {

        String domainUri = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";
        new TopicModeler(domainUri,helper, Resource.Type.ITEM).run();

    }

    @Test
    public void clean() throws InterruptedException {

        udm.delete(Resource.Type.ANY).all();

        udm.delete(Relation.Type.ANY).all();

    }



    @Test
    public void summary() throws IOException {

        LOG.info("Ready to build a summary");

        FileWriter writer = new FileWriter("target/repository-documents.csv");
        String separator = ";";

        writer.write("uri"+separator+"year"+separator+"title\n");

        udm.find(Resource.Type.DOCUMENT)
                .all()
                .stream()
                .map(res -> udm.read(Resource.Type.DOCUMENT).byUri(res.getUri()).get().asDocument())
                .forEach(document -> {
                    try {
                        String year = (Strings.isNullOrEmpty(document.getPublishedOn()))? "NONE" : document
                                .getPublishedOn
                                ();
                        String title = (Strings.isNullOrEmpty(document.getTitle()))? "unknown" : document.getTitle()
                                .replace(";",":").replace("\"","");
                        StringBuilder row = new StringBuilder()
                                .append(document.getUri()).append(separator)
                                .append(year).append(separator)
                                .append(title.length()>100? title.substring(0,100) : title)
                                ;
                        LOG.info(row.toString());
                        writer.write(row.toString());
                        writer.write("\n"); // newline
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

        writer.close();


    }


}
