/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.helper;

import lombok.Data;
import org.librairy.computing.cluster.Partitioner;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.modeler.lda.builder.*;
import org.librairy.boot.storage.UDM;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.boot.storage.system.column.repository.UnifiedColumnRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by cbadenes on 12/01/16.
 */
@Data
@Component
public class ModelingHelper {

    @Value("#{environment['LIBRAIRY_LDA_VOCABULARY_SIZE']?:${librairy.lda.vocabulary.size}}")
    Integer vocabSize;

    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    StorageHelper storageHelper;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    Partitioner partitioner;

    @Autowired
    DealsBuilder dealsBuilder;

    @Autowired
    SimilarityBuilder similarityBuilder;

    @Autowired
    TopicsBuilder topicsBuilder;

    @Autowired
    WorkspaceBuilder workspaceBuilder;

    @Autowired
    SQLHelper sqlHelper;

    @Autowired
    CassandraHelper cassandraHelper;

    @Autowired
    EventBus eventBus;

    @Autowired
    UDM udm;

    @Autowired
    UnifiedColumnRepository columnRepository;
}
