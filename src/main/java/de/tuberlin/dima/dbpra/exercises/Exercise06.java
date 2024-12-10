package de.tuberlin.dima.dbpra.exercises;

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * WICHTIG - Falls der Code nicht läuft:
 * Dieser Code wurde mit Java 17 getestet. Ich habe folgende Statements als
 * --add-opens=java.base/java.nio=ALL-UNNAMED
 * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 * --add-opens=java.base/java.lang=ALL-UNNAMED
 * --add-opens=java.base/java.util=ALL-UNNAMED
 * --add-opens=java.base/sun.security.action=ALL-UNNAMED
 *
 */

public class Exercise06 {

    /*************************************
     Task 1
     (See slides)

     Input: lineitem

     Output:
     [ShippingYear (↑), Amount]

     **************************************/
    public static JavaRDD<Tuple2<String, Integer>> ex01(JavaRDD<String> lineitem) {
        // Schritt 1: Filtere die Zeilen nach den gegebenen Kriterien
        JavaRDD<String> filteredLines = lineitem.filter(line -> {
            String[] parts = line.split("\\|");

            // Extrahiere die relevanten Felder
            String shipMode = parts[14];
            String status = parts[9];
            double quantity = Double.parseDouble(parts[4]);
            double tax = Double.parseDouble(parts[7]);

            // Prüfe alle Bedingungen
            return (shipMode.equals("TRUCK") || shipMode.equals("RAIL")) &&
                    status.equals("F") &&
                    quantity > 40 &&
                    tax >= 0.03;
        });

        // Schritt 2: Extrahiere das Versandjahr aus den gefilterten Zeilen
        JavaRDD<String> years = filteredLines.map(line -> {
            String[] parts = line.split("\\|");
            return parts[10].substring(0, 4); // Nimm die ersten 4 Zeichen des Datums
        });

        // Schritt 3: Zähle die Häufigkeit jedes Jahres
        JavaPairRDD<String, Integer> yearCounts = years
                .mapToPair(year -> new Tuple2<>(year, 1))
                .reduceByKey((a, b) -> a + b);

        // Schritt 4: Konvertiere zu finalem Format und sortiere nach Jahr aufsteigend
        return yearCounts
                .map(tuple -> tuple)
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
        // Schritt 1: Erstelle eine Zuordnung von Nations-ID zu Nationsnamen
        JavaPairRDD<String, String> nationPairs = nation.mapToPair(line -> {
            String[] parts = line.split("\\|");
            return new Tuple2<>(parts[0], parts[1]);
        });

        // Schritt 2: Erstelle Zuordnung von Nations-ID zu Lieferanteninformationen
        JavaPairRDD<String, Tuple2<String, String>> supplierPairs = supplier.mapToPair(line -> {
            String[] parts = line.split("\\|");
            // Speichere Name und Telefon als Tuple
            Tuple2<String, String> supplierInfo = new Tuple2<>(parts[1], parts[4]);
            return new Tuple2<>(parts[3], supplierInfo);
        });

        // Schritt 3: Verbinde die Datensätze und filtere nach gewünschten Nationen
        JavaRDD<Tuple3<String, String, String>> result = supplierPairs
                .join(nationPairs)
                .map(joined -> {
                    String name = joined._2()._1()._1();     // Lieferantenname
                    String phone = joined._2()._1()._2();    // Telefonnummer
                    String nationName = joined._2()._2();    // Nationsname
                    return new Tuple3<>(name, phone, nationName);
                })
                .filter(tuple ->
                        tuple._3().equals("GERMANY") ||
                                tuple._3().equals("CHINA")
                );

        // Schritt 4: Sortiere nach Lieferantenname
        return result.sortBy(tuple -> tuple._1(), true, 1);
    }

    /*************************************
     Task 3:
     (See slides)

     Input: customer, nation

     Ergebnisschema:
     [NationName(↓), MaxAcctbal]

     *************************************/
    public static JavaRDD<Tuple2<String, Double>> ex03(JavaRDD<String> customer, JavaRDD<String> nation) {
        // Schritt 1: Erstelle eine Zuordnung von Nations-ID zu Nationsnamen
        JavaPairRDD<String, String> nationPairs = nation.mapToPair(line -> {
            String[] parts = line.split("\\|");
            return new Tuple2<>(parts[0], parts[1]);
        });

        // Schritt 2: Extrahiere Kunden-Kontostand mit Nations-ID
        JavaPairRDD<String, Double> customerBalances = customer.mapToPair(line -> {
            String[] parts = line.split("\\|");
            return new Tuple2<>(parts[3], Double.parseDouble(parts[5]));
        });

        // Schritt 3: Berechne maximalen Kontostand pro Nation
        JavaPairRDD<String, Double> maxBalances = customerBalances
                .groupByKey()
                .mapToPair(group -> {
                    double maxBalance = -Double.MAX_VALUE;
                    for (Double balance : group._2()) {
                        maxBalance = Math.max(maxBalance, balance);
                    }
                    return new Tuple2<>(group._1(), maxBalance);
                });

        // Schritt 4: Verbinde mit Nationsnamen und wende Filter an
        JavaPairRDD<String, Double> resultPairs = maxBalances
                .join(nationPairs)
                .mapToPair(joined -> new Tuple2<>(joined._2()._2(), joined._2()._1()))
                .filter(tuple -> !tuple._1().equals("UNITED STATES"))
                .filter(tuple -> tuple._2() >= 9000.0);

        // Schritt 5: Sortiere nach Nationsname absteigend und konvertiere zu JavaRDD
        return resultPairs
                .map(tuple -> tuple)
                .sortBy(tuple -> tuple._1(), false, 1);
    }
}

