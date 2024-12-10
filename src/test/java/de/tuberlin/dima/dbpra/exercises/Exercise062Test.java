package de.tuberlin.dima.dbpra.exercises;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class Exercise062Test {

    @Test
    public void testEx2() throws Exception {
        System.out.println("Executing test for Exercise 2");

        SparkConf conf = new SparkConf().set("spark.ui.enabled", "false").setAppName("DBPRA").setMaster("local[4]");
        Logger.getLogger("org").setLevel(Level.ERROR);

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            ArrayList<String> input = new ArrayList<>();
            String line;

            BufferedReader brInput = new BufferedReader(new InputStreamReader(Exercise062Test.class.getClassLoader().getResourceAsStream("supplier.tbl")));
            while ((line = brInput.readLine()) != null) {
                input.add(line);
            }

            JavaRDD<String> supplierRDD = sc.parallelize(input);

            ArrayList<String> input2 = new ArrayList<>();
            brInput = new BufferedReader(new InputStreamReader(Exercise062Test.class.getClassLoader().getResourceAsStream("nation.tbl")));
            while ((line = brInput.readLine()) != null) {
                input2.add(line);
            }

            JavaRDD<String> nationRDD = sc.parallelize(input2);

            ArrayList<String> expectedResult = new ArrayList<>();

            BufferedReader brResult = new BufferedReader(new InputStreamReader(Exercise062Test.class.getClassLoader().getResourceAsStream("ex2.tbl")));
            while ((line = brResult.readLine()) != null) {
                expectedResult.add(line);
            }

            System.out.println("Executing exercise 2 code..");

            List<Tuple3<String, String, String>> actualResult = Exercise06.ex02(supplierRDD, nationRDD).collect();

            Assert.assertEquals(expectedResult.size(), actualResult.size());

            int i = 0;
            for (String eLine : expectedResult) {
                Tuple3<String, String, String> tuple = actualResult.get(i);
                String aLine = tuple._1() + "|" + tuple._2() + "|" + tuple._3();
                Assert.assertEquals(eLine, aLine);
                i++;
            }
        }
    }
}
