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

    public PatentsReferenceModel(){
        this.references = new ConcurrentHashMap();
    }


    public Set<String> load(String baseUri){
        String baseDir = "/Users/cbadenes/Documents/OEG/Projects/MINETUR/TopicModelling-2016/patentes-TIC-norteamericanas/";

        File outDir = Paths.get(baseDir, "metaFiles").toFile();

        if (!outDir.exists()){
            outDir.mkdirs();
        }


        File metaFiles = Paths.get(baseDir,"cites_uspto.csv").toFile();

        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);


        Set<String> domainURIs = new HashSet<>();
        int maxCites = 0;
        try {
            Files.lines(metaFiles.toPath()).forEach(line -> {


                StringTokenizer tokenizer = new StringTokenizer(line,"\t");

                String id           = tokenizer.nextToken();
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
                    String uri = baseUri + id;
                    domainURIs.add(uri);
                    references.put(uri,cites);

                }



            });

            return domainURIs;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    @Data
    public class TestSample{

        List<String> references;

        List<String> all;


    }


}
