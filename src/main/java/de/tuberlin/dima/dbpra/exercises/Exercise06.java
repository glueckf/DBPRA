package de.tuberlin.dima.dbpra.exercises;

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Exercise06 {

    /*************************************
     Task 1
     (See slides)

     Input: lineitem

     Output:
     [ShippingYear (↑), Amount]

     **************************************/
    public static JavaRDD<Tuple2<String, Integer>> ex01(JavaRDD<String> lineitem) {
        // Direkte Transformation der Daten in Jahr-Gruppierungen
        return lineitem
                // Transformiere und filtere in einem Schritt
                .flatMap(line -> {
                    String[] parts = line.split("\\|");

                    // Validiere erst alle Bedingungen
                    boolean validShipment = Arrays.asList("TRUCK", "RAIL").contains(parts[14]);
                    boolean validStatus = "F".equals(parts[9]);
                    boolean validQuantity = Double.parseDouble(parts[4]) > 40;
                    boolean validTax = Double.parseDouble(parts[7]) >= 0.03;

                    // Wenn alle Bedingungen erfüllt sind, gib das Jahr zurück
                    if (validShipment && validStatus && validQuantity && validTax) {
                        return Arrays.asList(parts[10].substring(0, 4)).iterator();
                    }
                    return Collections.emptyIterator();
                })
                // Zähle Vorkommen pro Jahr
                .mapToPair(year -> new Tuple2<>(year, 1))
                .groupByKey()
                .map(group -> new Tuple2<>(
                        group._1(),
                        Iterables.size(group._2())
                ))
                // Sortiere nach Jahr
                .sortBy(tuple -> tuple._1(), true, 1);
    }

    /*************************************
     Task 2:
     (See slides)

     Input: supplier, nation

     Output:
     [Name (↑), Phone, NationName]

     *************************************/
    public static JavaRDD<Tuple3<String, String, String>> ex02(JavaRDD<String> supplier, JavaRDD<String> nation) {
        // your code goes here
        throw new NotImplementedException();
    }

    /*************************************
     Task 3:
     (See slides)

     Input: customer, nation

     Ergebnisschema:
     [NationName(↓), MaxAcctbal]

     *************************************/
    public static JavaRDD<Tuple2<String, Double>> ex03(JavaRDD<String> customer, JavaRDD<String> nation) {
        // your code goes here
        throw new NotImplementedException();
    }
}
