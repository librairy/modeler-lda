package org.librairy.modeler.lda.models.topic;

import lombok.Getter;
import org.librairy.model.domain.relations.Relationship;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 01/05/16:
 *
 * @author cbadenes
 */
public class DensityDistribution implements Serializable {

    @Getter
    String uri;

    @Getter
    List<Relationship> relationships;

    public DensityDistribution(String uri, Iterable<WeightedPair> weightedPairs) {
        this.uri = uri;
        this.relationships = new ArrayList<>();

        if (weightedPairs != null){
            for (WeightedPair weightedPair : weightedPairs) {
                this.relationships.add(new Relationship(weightedPair.getUri2(),weightedPair.getWeight()));
            }
        }

    }
}

