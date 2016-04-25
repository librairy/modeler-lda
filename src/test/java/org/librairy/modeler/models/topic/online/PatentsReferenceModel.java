package org.librairy.modeler.models.topic.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 14/04/16:
 *
 * @author cbadenes
 */
public class PatentsReferenceModel {

    private static final Logger LOG = LoggerFactory.getLogger(PatentsReferenceModel.class);

    ConcurrentHashMap<String,List<String>> references;

    int minCites = 0;

    int maxCites = 0;

    public PatentsReferenceModel(){
        this.references = new ConcurrentHashMap();
    }


    public Set<String> load(String baseUri){

        String baseDir = "/Users/cbadenes/Documents/OEG/Projects/MINETUR/TopicModelling-2016/patentes-TIC-norteamericanas/";

        LOG.info("Loading reference patents from : " + baseDir);

//        File outDir = Paths.get(baseDir, "metaFiles").toFile();
//
//        if (!outDir.exists()){
//            outDir.mkdirs();
//        }


        File metaFiles = Paths.get(baseDir,"cites_uspto.csv").toFile();

//        ObjectMapper jsonMapper = new ObjectMapper();
//        jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);

        LOG.info("Exists file: " + metaFiles.getAbsolutePath() + " ->" + metaFiles.exists());

        Set<String> domainURIs = new HashSet<>();
        try {
            Files.lines(metaFiles.toPath()).forEach(line -> {


                StringTokenizer tokenizer = new StringTokenizer(line,"\t");

                String id           = tokenizer.nextToken();
                String uri = baseUri + id;

                String x            = tokenizer.nextToken();
                String title        = tokenizer.nextToken();
                String date         = tokenizer.nextToken();
                String type         = tokenizer.nextToken();
                if ( tokenizer.hasMoreTokens()){
                    String categories   = tokenizer.nextToken();
                }


                // Only takes referenced
                List<String> cites = new ArrayList<String>();
                if ( tokenizer.hasMoreTokens()){
                    String citesString    = tokenizer.nextToken();

                    StringTokenizer valueTokenizer = new StringTokenizer(citesString,"|");
                    while(valueTokenizer.hasMoreTokens()){
                        String refId    =  valueTokenizer.nextToken();
                        String refUri   =  (refId.startsWith("H"))? baseUri + refId :  baseUri + "0" + refId;
                        domainURIs.add(refUri);
                        cites.add(refUri);
                    }
                    if (maxCites < cites.size()) maxCites = cites.size();
                    references.put(uri,cites);
                }
                domainURIs.add(uri);
            });

            return domainURIs;

        } catch (IOException e) {
            LOG.warn("Error reading file",e);
            throw new RuntimeException(e);
        }
    }


    public boolean contains(String uri){
        return references.contains(uri);
    }

    public Integer getSize(){
        return this.references.size();
    }

    public Integer getNumberOfPatentsWithReferences(){
        return Long.valueOf(this.references.entrySet().stream().filter(entry -> entry.getValue() != null && !entry
                .getValue().isEmpty()).count()).intValue();
    }

    public TestSample sampleOf(Integer size, boolean withReferences){
        Random       random    = new Random();
        List<String> refs    = new ArrayList<>();
        Set<String>  all     = new HashSet<>();

        List<String> keys      = new ArrayList<String>(references.keySet());
        for (int i = 0; i< keys.size(); i++){
            if (refs.size() == size) break;
            String key = keys.get( random.nextInt(keys.size()));
            List<String> related = references.get(key);
            if (withReferences && related.isEmpty()) continue;
            all.add(key);
            all.addAll(related);
            refs.add(key);
        }

        TestSample testSample = new TestSample();
        testSample.setReferences(refs);
        testSample.setAll(new ArrayList<>(all));
        return testSample;
    }


    public List<String> getRefs(String uri){
        List<String> result = references.get(uri);
        if (result == null){
            return Collections.EMPTY_LIST;
        }
        return result;
    }

    public static void main(String[] args){
        PatentsReferenceModel model = new PatentsReferenceModel();
        Set<String> uris = model.load("http://librairy.org/documents/");

        System.out.println("Domain URIs: " + uris.size());

        uris.stream().forEach(uri -> model.getRefs(uri));

    }

    @Override
    public String toString() {
        StringBuilder description = new StringBuilder();

        description.append("Number of Patents: ").append(getSize()).append("|");
        description.append("Number of Patents with cites: ").append(getNumberOfPatentsWithReferences()).append("|");
        description.append("Max cites per Patents: ").append(maxCites).append("|");
        description.append("Min cites per Patents: ").append(minCites).append("|");

        return description.toString();
    }

    @Data
    public class TestSample{

        List<String> references;

        List<String> all;


    }


}
