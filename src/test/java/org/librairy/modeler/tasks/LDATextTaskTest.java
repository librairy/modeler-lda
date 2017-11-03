/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.tasks;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.SimilarResource;
import org.librairy.modeler.lda.models.Text;
import org.librairy.modeler.lda.tasks.LDATextTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

/**
 * Created on 27/06/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
//@TestPropertySource(properties = {
//        "librairy.lda.event.value = 60000"
//})
public class LDATextTaskTest {


    private static final Logger LOG = LoggerFactory.getLogger(LDATextTaskTest.class);

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    DealsBuilder dealsBuilder;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    ModelingHelper helper;

    @Test
    public void execute(){

        Text text1 = new Text("sample", "A hat-trick in rugby union, the scoring of three tries or three drop goals " +
                "in a single match, has been achieved 52 times in the history of the Six Nations Championship. The annual competition, established in 1882, was originally known as the Home Nations Championship and contested between England, Ireland, Scotland and Wales. It was expanded to the Five Nations when France joined in 1910,[A] and then to the Six Nations with the addition of Italy in 2000.");

        getSimilars(text1);


        Text text2 = new Text("sample","Caches are tremendously useful in a wide variety of use cases. For example, " +
                "you should consider using caches when a value is expensive to compute or retrieve, and you will need" +
                " its value on a certain input more than once.\n" +
                "\n" +
                "A Cache is similar to ConcurrentMap, but not quite the same. The most fundamental difference is that" +
                " a ConcurrentMap persists all elements that are added to it until they are explicitly removed. A " +
                "Cache on the other hand is generally configured to evict entries automatically, in order to " +
                "constrain its memory footprint. In some cases a LoadingCache can be useful even if it doesn't evict " +
                "entries, due to its automatic cache loading.");
        getSimilars(text2);



    }


    private void getSimilars(Text text){
        Integer topValues = 10;

        String domainUri = "http://librairy.org/domains/90b559119ab48e8cf4310bf92f6b4eab";

        List<Resource.Type> types = Arrays.asList(new Resource.Type[]{Resource.Type.ITEM});

        List<SimilarResource> similarResources = new LDATextTask(helper).getSimilar
                (text, topValues, domainUri, types);


        similarResources.forEach( comparison -> LOG.info("Comparison: " + comparison));
    }

}
