/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.domain.resources.Resource;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class RelationBuilder implements Serializable{

    public static Relation newSimilarTo(Resource.Type type, String uri1, String uri2, String domainUri, Double weight){

        Relation relation;
        switch(type){
            case DOCUMENT: relation = Relation.newSimilarToDocuments(uri1,uri2,domainUri);
                break;
            case ITEM: relation = Relation.newSimilarToItems(uri1,uri2,domainUri);
                break;
            case PART: relation = Relation.newSimilarToParts(uri1,uri2,domainUri);
                break;
            default: throw new RuntimeException("No similarTo relation found for type: " + type);
        }
        relation.setWeight(weight);
        return relation;
    }
}
