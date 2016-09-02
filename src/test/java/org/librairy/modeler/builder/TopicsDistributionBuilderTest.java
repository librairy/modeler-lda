package org.librairy.modeler.builder;

import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.*;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.*;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
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
        "librairy.columndb.host = wiener.dia.fi.upm.es",
        "librairy.columndb.port = 5011",
        "librairy.documentdb.host = wiener.dia.fi.upm.es",
        "librairy.documentdb.port = 5021",
        "librairy.graphdb.host = wiener.dia.fi.upm.es",
        "librairy.graphdb.port = 5030",
        "librairy.lda.maxiterations = 3",
        "librairy.lda.vocabulary.size = 10000"
})
public class TopicsDistributionBuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(TopicsDistributionBuilderTest.class);

    @Autowired
    TopicsDistributionBuilder topicsDistributionBuilder;

    @Autowired
    SimilarityBuilder similarityBuilder;


    String domainURI = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";

    @Test
    public void inference(){

        Text text = new Text();
        text.setId("t1");
        text.setContent("In 1968, Arthur Appel described the first algorithm for what would eventually become known as ray " +
                "casting - a basis point for almost all of modern 3D graphics, as well as the later pursuit of " +
                "photorealism in graphics.");

        List<Text> texts = Arrays.asList(new Text[]{text});

        LOG.info("Topics Distribution:");
        topicsDistributionBuilder.inference
                (domainURI, texts).forEach(d -> LOG.info("> "+d));

    }

    @Test
    public void topSimilars(){

        Text text = new Text();
        text.setId("t1");
        text.setContent("In 1968, Arthur Appel described the first algorithm for what would eventually become known as ray " +
                "casting - a basis point for almost all of modern 3D graphics, as well as the later pursuit of " +
                "photorealism in graphics.");

        List<Text> texts = Arrays.asList(new Text[]{text});


        LOG.info("Topics Distribution:");
        TopicsDistribution inference = topicsDistributionBuilder.inference(domainURI, texts).get(0);
        List<SimilarResource> similarItems = similarityBuilder.topSimilars(Resource.Type.ITEM, domainURI, 10,
                inference.getTopics());

        similarItems.forEach(si -> LOG.info("Similar to: " + si));

    }

}
