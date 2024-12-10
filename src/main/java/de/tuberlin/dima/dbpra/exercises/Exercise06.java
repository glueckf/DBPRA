package de.tuberlin.dima.dbpra.exercises;

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.StreamSupport;

public class Exercise06 {

    /*************************************
     Task 1
     (See slides)

     Input: lineitem

     Output:
     [ShippingYear (↑), Amount]

     **************************************/
    public static JavaRDD<Tuple2<String, Integer>> ex01(JavaRDD<String> lineitem) {
        // Transformiere die Zeilen des lineitem-Datasets, filtere sie und gruppiere sie nach Versandjahr
        return lineitem
                .flatMap(line -> {
                    // Zerlege jede Zeile anhand des Trennzeichens "|"
                    String[] parts = line.split("\\|");

                    // Überprüfe, ob die Zeile die angegebenen Filterkriterien erfüllt
                    boolean validShipment = Arrays.asList("TRUCK", "RAIL").contains(parts[14]); // Nur bestimmte Versandarten
                    boolean validStatus = "F".equals(parts[9]); // Nur vollständige Lieferungen
                    boolean validQuantity = Double.parseDouble(parts[4]) > 40; // Mindestmenge
                    boolean validTax = Double.parseDouble(parts[7]) >= 0.03; // Mindeststeuer

                    // Wenn alle Kriterien erfüllt sind, gib das Jahr des Versands zurück
                    if (validShipment && validStatus && validQuantity && validTax) {
                        return Arrays.asList(parts[10].substring(0, 4)).iterator();
                    }
                    return Collections.emptyIterator(); // Keine Ausgabe, wenn Bedingungen nicht erfüllt sind
                })
                // Zähle die Vorkommen jedes Jahres
                .mapToPair(year -> new Tuple2<>(year, 1))
                .groupByKey()
                .map(group -> new Tuple2<>(
                        group._1(), // Versandjahr
                        Iterables.size(group._2()) // Anzahl der Vorkommen
                ))
                // Sortiere die Ausgabe nach Versandjahr (aufsteigend)
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
        // Erstelle eine Zuordnung von NationID zu NationName
        JavaPairRDD<String, String> nationMap = nation.mapToPair(line -> {
            String[] parts = line.split("\\|");
            return new Tuple2<>(parts[0], parts[1]);
        });

        // Transformiere die Lieferantendaten zu einer Zuordnung von NationID zu (Name, Telefon)
        JavaPairRDD<String, Tuple2<String, String>> supplierMap = supplier.mapToPair(line -> {
            String[] parts = line.split("\\|");
            return new Tuple2<>(parts[3], new Tuple2<>(parts[1], parts[4]));
        });

        // Führe die Daten zusammen, filtere nach relevanten Nationen und forme sie in das gewünschte Format um
        return supplierMap
                .join(nationMap) // Verknüpfe Lieferantendaten mit Nationdaten basierend auf der NationID
                .filter(tuple -> {
                    String nationName = tuple._2()._2();
                    // Behalte nur Lieferanten aus "GERMANY" oder "CHINA"
                    return nationName.equals("GERMANY") || nationName.equals("CHINA");
                })
                .map(tuple -> {
                    // Extrahiere Name, Telefon und Nationname aus den verbundenen Daten
                    String name = tuple._2()._1()._1(); // Name des Lieferanten
                    String phone = tuple._2()._1()._2(); // Telefonnummer des Lieferanten
                    String nationName = tuple._2()._2(); // Name der Nation
                    return new Tuple3<>(name, phone, nationName);
                })
                // Sortiere die Ergebnisse alphabetisch nach dem Namen des Lieferanten
                .sortBy(tuple -> tuple._1(), true, 1);
    }

    /*************************************
     Task 3:
     (See slides)

     Input: customer, nation

     Ergebnisschema:
     [NationName(↓), MaxAcctbal]

     *************************************/
    public static JavaRDD<Tuple2<String, Double>> ex03(JavaRDD<String> customer, JavaRDD<String> nation) {
        // Erstelle eine Zuordnung von NationID zu NationName
        JavaPairRDD<String, String> nationMap = nation.mapToPair(line -> {
            String[] parts = line.split("\\|");
            return new Tuple2<>(parts[0], parts[1]);
        });

        // Transformiere die Kundendaten zu einer Zuordnung von NationID zu Konto-Balance
        JavaPairRDD<String, Double> customerMap = customer.mapToPair(line -> {
            String[] parts = line.split("\\|");
            String nationID = parts[3]; // NationID
            double balance = Double.parseDouble(parts[5]); // Konto-Balance
            return new Tuple2<>(nationID, balance);
        });

        // Finde das maximale Konto-Balance für jede Nation
        JavaPairRDD<String, Double> nationBalances = customerMap
                .groupByKey()
                .mapToPair(tuple -> {
                    double maxBalance = StreamSupport.stream(tuple._2().spliterator(), false)
                            .mapToDouble(d -> d)
                            .max()
                            .orElse(0.0); // Falls keine Werte vorhanden, setze 0.0
                    return new Tuple2<>(tuple._1(), maxBalance);
                });

        // Verknüpfe die maximalen Konto-Balancen mit den Nationennamen, filtere und sortiere die Ergebnisse
        return nationBalances
                .join(nationMap) // Verknüpfe NationID mit NationName
                .mapToPair(tuple -> new Tuple2<>(tuple._2()._2(), tuple._2()._1())) // (NationName, MaxBalance)
                .filter(tuple -> !tuple._1().equals("UNITED STATES")) // Entferne Einträge aus den USA
                .filter(tuple -> tuple._2() >= 9000.0) // Behalte nur Balancen >= 9000.0
                .sortByKey(false) // Sortiere absteigend nach NationName
                .map(tuple -> tuple); // Formatiere in das endgültige Ausgabeformat
    }
}

