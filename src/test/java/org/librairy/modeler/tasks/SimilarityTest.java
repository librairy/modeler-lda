/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.tasks;

import cc.mallet.util.Maths;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Doubles;
import org.apache.commons.text.similarity.HammingDistance;
import org.junit.Test;
import org.librairy.metrics.distance.JensenShannonDivergence;
import org.librairy.metrics.similarity.CosineSimilarity;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class SimilarityTest {
    Integer numVectors  = 10;
    Integer numSimilars = 5;
    Integer numTopics   = 5;
    Double threshold    = 0.75;
    Double ratio        = 0.99;

    private List<Distribution> createGoldStandard(){
        List<Distribution> vectors = new ArrayList<>();

        // Create Sampling: generate vectors
        for (int i=0;i<numVectors;i++){
            List<Double> vector = createVector(numTopics);
            Distribution distribution = new Distribution(vector,String.valueOf(i));
            vectors.add(distribution);

        }
        return vectors;
    }

    private Map<String,List<Distribution>> createBits(List<Distribution> vectors){
        Map<String,List<Distribution>> vectorsByBits = new HashMap<>();

        // Create Sampling: generate vectors
        for (Distribution distribution: vectors){
            String bits = convertToBits(distribution.getVector());
            Double sum  = distribution.getVector().stream().reduce((a,b)->a+b).get();

            List<Distribution> partialVectors = new ArrayList<>();
            if (vectorsByBits.containsKey(bits)){
                partialVectors = vectorsByBits.get(bits);
            }
            partialVectors.add(distribution);
            vectorsByBits.put(bits,partialVectors);
            System.out.println("Distribution: [" +distribution.getId()+"]"+ "- ["+bits+"] - [Sum:"+sum+"] - [NumChanges:" + numChanges(bits) +"] -  " +  distribution.getVector());
        }

        return vectorsByBits;
    }

    @Test
    public void testBitsSimilarity(){

        List<Distribution> vectors = createGoldStandard();
        Map<String,List<Distribution>> vectorsByBits = createBits(vectors);



        // calculate similarities
        Integer tp = 0;
        Integer tn = 0;
        Integer fp = 0;
        Integer fn = 0;
        HammingDistance hd = new HammingDistance();

        for(int i=0;i<vectors.size();i++){
            Distribution refVector = vectors.get(i);
            System.out.println("["+refVector.getId()+"]:");


            // JSD common
            List<Tuple2<Distribution, Double>> similaritiesBasedOnDistribution = vectors.stream()
                    .filter(v -> !v.equals(refVector))
                    .map(a -> new Tuple2<Distribution, Double>(a, JensenShannonSimilarity.apply(Doubles.toArray(refVector.getVector()), Doubles.toArray(a.getVector()))))
                    .sorted((a, b) -> -new Double(a._2).compareTo(b._2))
                    .filter( r -> r._2 > threshold)
                    .limit(numSimilars)
                    .collect(Collectors.toList());

            System.out.println("BasedOnDistribution: \t\t" + similaritiesBasedOnDistribution.stream().map(d -> "["+d._1.getId()+":"+d._2+"]").collect(Collectors.joining(" -> ")));

            List<Tuple2<Distribution, Double>> similaritiesBasedOnJSD = vectors.stream()
                    .filter(v -> !v.equals(refVector))
                    .map(a -> new Tuple2<Distribution, Double>(a,   Maths.jensenShannonDivergence(Doubles.toArray(refVector.getVector()), Doubles.toArray(a.getVector()))))
                    .sorted((a, b) -> -new Double(a._2).compareTo(b._2))
                    .limit(numSimilars)
                    .collect(Collectors.toList());

            System.out.println("BasedOnJSD: \t\t\t\t" + similaritiesBasedOnJSD.stream().map(d -> "["+d._1.getId()+":"+d._2+"]").collect(Collectors.joining(" -> ")));

            List<Tuple2<Distribution, Double>> similaritiesBasedOnCosineSim = vectors.stream()
                    .filter(v -> !v.equals(refVector))
                    .map(a -> new Tuple2<Distribution, Double>(a,   CosineSimilarity.apply(Doubles.toArray(refVector.getVector()), Doubles.toArray(a.getVector()))))
                    .sorted((a, b) -> -new Double(a._2).compareTo(b._2))
                    .limit(numSimilars)
                    .collect(Collectors.toList());

            System.out.println("BasedOnCosineSimilarity: \t" + similaritiesBasedOnCosineSim.stream().map(d -> "["+d._1.getId()+":"+d._2+"]").collect(Collectors.joining(" -> ")));

            // Based on Bits
            List<Tuple2<Distribution, Double>> similaritiesBasedOnBits = new ArrayList<>();
            String bits = convertToBits(refVector.getVector());
            if (vectorsByBits.containsKey(bits)){
                similaritiesBasedOnBits = vectorsByBits.get(bits).stream()
                        .filter(v -> !v.equals(refVector))
                        .map(a -> new Tuple2<Distribution, Double>(a, JensenShannonSimilarity.apply(Doubles.toArray(refVector.getVector()), Doubles.toArray(a.getVector()))))
                        .sorted((a, b) -> -new Double(a._2).compareTo(b._2))
                        .limit(numSimilars)
                        .collect(Collectors.toList());
            }
//            //extra
//            for (String neigbour : nearestNeighboursBits(bits)){
//                if (vectorsByBits.containsKey(neigbour)){
//                    System.out.println("added from: " + neigbour);
//                    similaritiesBasedOnBits.addAll(vectorsByBits.get(neigbour).stream()
//                            .filter(v -> !v.equals(refVector))
//                            .map(a -> new Tuple2<Distribution, Double>(a, JensenShannonSimilarity.apply(Doubles.toArray(refVector.getVector()), Doubles.toArray(a.getVector()))))
//                            .sorted((a, b) -> -new Double(a._2).compareTo(b._2))
//                            .limit(numSimilars)
//                            .collect(Collectors.toList()));
//                }
//
//            }
//            similaritiesBasedOnBits = similaritiesBasedOnBits.stream().sorted((a,b) -> -a._2.compareTo(b._2)).limit(numSimilars).collect(Collectors.toList());
            System.out.println("BasedOnBits: \t\t\t\t" + similaritiesBasedOnBits.stream().map(d -> "["+d._1.getId()+":"+d._2+"]").collect(Collectors.joining(" -> ")));

            // Evaluation
            for(Tuple2<Distribution,Double> similarity: similaritiesBasedOnDistribution){
                if (similaritiesBasedOnBits.contains(similarity)) tp += 1;
                else fn += 1;
            }
            for(Tuple2<Distribution,Double> similarity: similaritiesBasedOnBits){
                if (!similaritiesBasedOnDistribution.contains(similarity)) fp += 1;
            }
            for(int k=0; k<numVectors; k++){
                int finalK = k;
                if ((similaritiesBasedOnDistribution.stream().filter(d -> d._1.getId().equals(String.valueOf(finalK))).count() == 0 )
                    && (similaritiesBasedOnBits.stream().filter(d -> d._1.getId().equals(String.valueOf(finalK))).count() == 0 )) tn += 1;
            }



        }

        Map<String, Long> groups = vectors.stream().map(d -> convertToBits(d.getVector())).collect(Collectors.groupingBy(String::toString, Collectors.counting()));

        long numCalculatedSimilarities = 0;
        for (Map.Entry<String,Long> entry: groups.entrySet()){
            System.out.println("Cluster: " + entry.getKey() + " => " + entry.getValue());
            numCalculatedSimilarities += entry.getValue()*entry.getValue();
        }

        System.out.println("TP="+tp);
        System.out.println("TN="+tn);
        System.out.println("FP="+fp);
        System.out.println("FN="+fn);


        Double precision    = Double.valueOf(tp) / (Double.valueOf(tp) + Double.valueOf(fp));
        Double recall       = Double.valueOf(tp) / (Double.valueOf(tp) + Double.valueOf(fn));

        System.out.println("Threshold: " + threshold);
        System.out.println("Precision@"+numSimilars+"=" + precision);
        System.out.println("Recall@"+numSimilars+"=" + recall);
        long clusters =  vectors.stream().map(d -> convertToBits(d.getVector())).distinct().count();
        System.out.println("Num Groups: " + clusters);
        System.out.println("Num Vectors: " + numVectors);
        System.out.println("Num Topics: " + numTopics);
        System.out.println("Ratio: " + ratio);



        long numTotalSimilarities       = numVectors*numVectors;
//        long numCalculatedSimilarities  = ((numVectors/clusters)*(numVectors/clusters))*clusters;
        long numSimilaritiesAvoid       = numTotalSimilarities-numCalculatedSimilarities;
        Double improvement                  = (Double.valueOf(numSimilaritiesAvoid)*100.0)/Double.valueOf(numTotalSimilarities);
        System.out.println("Total Similarities: " + numTotalSimilarities);
        System.out.println("Similarities Calculated: " + numCalculatedSimilarities);
        System.out.println("Saving: " + improvement +"%");

    }


    private List<Double> createVector(Integer dim){

        Double[] vector = new Double[dim];


        List<Integer> indexes = new ArrayList<>();
        for (int i=0;i<dim;i++){
            indexes.add(i);
        }

        Random random = new Random();
        Double acc = new Double(0.0);
        while(!indexes.isEmpty()){

            Collections.shuffle(indexes);

            Integer index = indexes.get(0);
            Double limit = 1.0 - acc;
            Double partialLimit = (80.0*limit)/100.0;
            Double val = Math.random()*partialLimit;
            if (indexes.size() == 1){
                val = limit;
            }
            if (val == 0.0){
                val = limit/indexes.size();
            }
            vector[index]= val;
            acc += val;
            indexes.remove(index);
        }

        return Arrays.asList(vector);

    }

    private String convertToBits(List<Double> vector){

        StringBuilder bits = new StringBuilder();


        Double last = vector.get(0);
        Double limit = 0.0;
        Double acc = last;
        for(int i=1; i< vector.size(); i++){
            limit = Math.max(last, Math.abs(1.0-acc));
            Double current = vector.get(i);
//            bits.append(current>last?"1":"0");

            Double slope = current-last;
            if (Math.abs(slope) > (ratio * limit)){
                if (slope > 0) bits.append("1");
                else bits.append("2");
            }else bits.append("0");

            last = current;
            acc += current;
//            limit -= current;
        }

        return bits.toString();

    }

    private List<String> nearestNeighboursBits(String bits){
        List<String> neighbours = new ArrayList<>();

        int index = 0;
        for (int i=0; i<bits.length();i++){
            if (bits.substring(index,i+1).equals("0")){
                String pre = bits.substring(0,index);
                String post = bits.substring(i+1,bits.length());
                neighbours.add(pre+"1"+post);
            }
            index += 1;
        }

        return neighbours;
    }

    @Test
    public void truncateBits(){
        String bits = "10001";
        System.out.println(bits);

        System.out.println("Permutations:");
        nearestNeighboursBits(bits).forEach( b -> System.out.println(b));
    }

    private Integer numChanges(String bits){
        char[] chars = bits.toCharArray();
        char last = chars[0];
        Integer changes = 0;
        for(int i=1; i< chars.length; i++){
            if (chars[i] != last) changes+=1;
            last = chars[i];
        }
        return changes;
    }

    @Test
    public void countChanges(){

        String bits = "10001";
        System.out.println(bits);
        System.out.println("Num Changes: " + numChanges(bits));

    }

    private List<String> permutations(String bits){
        List<String> result = Arrays.asList(bits.split("(?!^)"));
        Collection<List<String>> permutations = Collections2.permutations(result);
        return permutations.stream().map(l -> l.stream().collect(Collectors.joining())).distinct().collect(Collectors.toList());
    }

    @Test
    public void permutateBits(){
        String bits = "10001";
        System.out.println(bits);

        permutations(bits).forEach( b -> System.out.println(b));
    }


    @Test
    public void jensenDistance(){

        List<Double> v1 = createVector(10);
        List<Double> v2 = createVector(10);
        System.out.println("V1: " + v1);
        System.out.println("V2: " + v2);

        double divergence = Maths.jensenShannonDivergence(Doubles.toArray(v1), Doubles.toArray(v2));
        System.out.println("Result: " + divergence);

        double similarity = JensenShannonSimilarity.apply(Doubles.toArray(v1), Doubles.toArray(v2));
        System.out.println("Similarity: " + similarity);

        double divergence2 = JensenShannonDivergence.apply(Doubles.toArray(v1), Doubles.toArray(v2));
        System.out.println("Result2: " + divergence2);
    }

    private static Double Log2(Double n) {
        return Math.log(n) / Math.log(2);
    }

    private Double entropy(List<Double> probability){
        Double entropy = 0.0;
        Double total = Double.valueOf(probability.size());
        for (Double count: probability) {
            Double prob =  count / total;
            entropy -= prob * Log2(prob);
        }
        return entropy;
    }

    @Test
    public void entropyTest(){

        for(int i=0;i<5;i++){
            List<Double> v = createVector(10);
            Double entropy = entropy(v);
            System.out.println("Vector: " + v);
            System.out.println("Entropy: " + entropy);
        }
    }


    @Test
    public void convertToBitsVector(){

        String expression = "0.012436885446414081, 0.6480727703548648, 0.07729304496747545, 0.2362934970506643, 0.025903802180581453";

        List<Double> vector = Arrays.stream(expression.split(",")).map(s -> Double.valueOf(s)).collect(Collectors.toList());

        String bits = convertToBits(vector);

        System.out.println(bits);
        
    }

}

