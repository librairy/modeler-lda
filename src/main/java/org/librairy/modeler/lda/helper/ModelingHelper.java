/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.helper;

import lombok.Data;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.boot.storage.dao.DomainsDao;
import org.librairy.boot.storage.dao.ParametersDao;
import org.librairy.computing.cluster.Partitioner;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.builder.WorkspaceBuilder;
import org.librairy.modeler.lda.dao.ComparisonsDao;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.TopicsDao;
import org.librairy.modeler.lda.services.SimilarityService;
import org.librairy.modeler.lda.utils.UnifiedExecutor;
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
    WorkspaceBuilder workspaceBuilder;

    @Autowired
    SQLHelper sqlHelper;

    @Autowired
    CassandraHelper cassandraHelper;

    @Autowired
    UnifiedExecutor unifiedExecutor;

    @Autowired
    EventBus eventBus;

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ShapesDao shapesDao;

    @Autowired
    CounterDao counterDao;

    @Autowired
    SimilarityService similarityService;

    @Autowired
    DomainsDao domainsDao;

    @Autowired
    TopicsDao topicsDao;

    @Autowired
    ComparisonsDao comparisonsDao;

    @Autowired
    ParametersDao parametersDao;

}
