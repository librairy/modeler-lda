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
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.builder.TopicsBuilder;
import org.librairy.modeler.lda.builder.TopicsDistributionBuilder;
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
public class TopicsDistributionBuilderTest {


    private static final Logger LOG = LoggerFactory.getLogger(TopicsDistributionBuilderTest.class);

    @Autowired
    TopicsDistributionBuilder topicsDistributionBuilder;


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

}
