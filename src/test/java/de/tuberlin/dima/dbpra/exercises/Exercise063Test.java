package de.tuberlin.dima.dbpra.exercises;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class Exercise063Test {

    @Test
    public void testEx3() throws Exception {
        System.out.println("Executing test for Exercise 3");

        SparkConf conf = new SparkConf().set("spark.ui.enabled", "false").setAppName("DBPRA").setMaster("local[4]");
        Logger.getLogger("org").setLevel(Level.ERROR);

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            ArrayList<String> input = new ArrayList<>();
            String line;

            BufferedReader brInput = new BufferedReader(new InputStreamReader(Exercise063Test.class.getClassLoader().getResourceAsStream("customer.tbl")));
            while ((line = brInput.readLine()) != null) {
                input.add(line);
            }

            JavaRDD<String> customerRDD = sc.parallelize(input);

            ArrayList<String> input2 = new ArrayList<>();
            brInput = new BufferedReader(new InputStreamReader(Exercise063Test.class.getClassLoader().getResourceAsStream("nation.tbl")));
            while ((line = brInput.readLine()) != null) {
                input2.add(line);
            }

            JavaRDD<String> nationRDD = sc.parallelize(input2);

            ArrayList<String> expectedResult = new ArrayList<>();

            BufferedReader brResult = new BufferedReader(new InputStreamReader(Exercise063Test.class.getClassLoader().getResourceAsStream("ex3.tbl")));
            while ((line = brResult.readLine()) != null) {
                expectedResult.add(line);
            }

            System.out.println("Executing exercise 3 code..");

            List<Tuple2<String, Double>> actualResult = Exercise06.ex03(customerRDD, nationRDD).collect();

            Assert.assertEquals(expectedResult.size(), actualResult.size());

            int i = 0;
            for (String eLine : expectedResult) {
                Tuple2<String, Double> tuple = actualResult.get(i);
                String aLine = tuple._1() + "|" + tuple._2();
                Assert.assertEquals(eLine, aLine);
                i++;
            }
        }
    }
}
