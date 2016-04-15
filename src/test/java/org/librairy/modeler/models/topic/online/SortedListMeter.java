package org.librairy.modeler.models.topic.online;

import java.util.Arrays;
import java.util.List;

/**
 * Created on 14/04/16:
 *
 * @author cbadenes
 */
public class SortedListMeter {

    public static double measure(List<String> reference, List<String> values){

        double T = values.size();
        double n = reference.size();
        double accum = 0;
        for (int i = 1; i <= n; i++){
            String refId = reference.get(i-1);
            double xi = values.indexOf(refId)+1;
            if (xi == 0){
                xi = T+1;
            }
            accum += i - xi;
        }

        return ((T*n)+accum)/(T*n);
    }


    public static final void main(String[] args){


        List<String> ref = Arrays.asList(new String[]{"4","1","3","2"});
        List<String> values = Arrays.asList(new String[]{"4","2"});

        double value = measure(ref, values);
        System.out.println(value);

    }

}
