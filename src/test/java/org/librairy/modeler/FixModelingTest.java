/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler;

import com.google.common.base.Strings;
import es.cbadenes.lab.test.IntegrationTest;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.SimilarityBuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.LDATrainingTask;
import org.librairy.boot.storage.UDM;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by cbadenes on 13/01/16.
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
//@TestPropertySource(properties = {
//})
public class FixModelingTest {

    private static final Logger LOG = LoggerFactory.getLogger(FixModelingTest.class);

    @Autowired
    ModelingHelper helper;

    @Autowired
    UDM udm;

    @Autowired
    SimilarityBuilder similarityBuilder;

    @Test
    public void calculateSimilarities() throws InterruptedException {

        String domainUri = "http://librairy.org/domains/default";

        similarityBuilder.discover(domainUri, Resource.Type.PART);

    }


    @Test
    public void topicModel() throws InterruptedException {

        String domainUri = "http://drinventor.eu/domains/4f56ab24bb6d815a48b8968a3b157470";
        new LDATrainingTask(domainUri, helper).run();

    }

    @Test
    public void clean() throws InterruptedException {

        udm.delete(Resource.Type.ANY).all();

        udm.delete(Relation.Type.ANY).all();

    }



    @Test
    public void summary() throws IOException {

        LOG.info("Ready to discover a summary");

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
