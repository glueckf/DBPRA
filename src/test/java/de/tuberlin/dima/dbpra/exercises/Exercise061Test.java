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


public class Exercise061Test {

    @Test
    public void testEx1() throws Exception {
        System.out.println("Executing test for Exercise 1");

        SparkConf conf = new SparkConf().set("spark.ui.enabled", "false").setAppName("DBPRA").setMaster("local[4]");
        Logger.getLogger("org").setLevel(Level.ERROR);

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            ArrayList<String> input = new ArrayList<>();
            String line;

            BufferedReader brInput = new BufferedReader(new InputStreamReader(Exercise061Test.class.getClassLoader().getResourceAsStream("lineitem.tbl")));
            while ((line = brInput.readLine()) != null) {
                input.add(line);
            }

            JavaRDD<String> lineItemRDD = sc.parallelize(input);

            ArrayList<String> expectedResult = new ArrayList<>();

            BufferedReader brResult = new BufferedReader(new InputStreamReader(Exercise061Test.class.getClassLoader().getResourceAsStream("ex1.tbl")));
            while ((line = brResult.readLine()) != null) {
                expectedResult.add(line);
            }

            System.out.println("Executing exercise 1 code..");
            List<Tuple2<String, Integer>> actualResult = Exercise06.ex01(lineItemRDD).collect();

            Assert.assertEquals(expectedResult.size(), actualResult.size());

            int i = 0;
            for (String eLine : expectedResult) {
                Tuple2<String, Integer> tuple = actualResult.get(i);
                String aLine = tuple._1 + "|" + tuple._2;
                Assert.assertEquals(eLine, aLine);
                i++;
            }
        }
    }
}
