/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 31/08/16:
 *
 * @author cbadenes
 */
@Data
public class TopicDescription {

    private final String id;

    private List<WordDescription> words = new ArrayList<>();

    public TopicDescription(String id){
        this.id = id;
    }

    public TopicDescription add(WordDescription wordDescription){
        this.words.add(wordDescription);
        return this;
    }

    public TopicDescription add(String word, Double weight){
        this.words.add(new WordDescription(word,weight));
        return this;
    }

    public String getContent(){
        return this.getWords().stream().map(wd -> wd.getWord()).collect(Collectors.joining(","));
    }
}
