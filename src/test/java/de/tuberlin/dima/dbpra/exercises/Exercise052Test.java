package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.config.ConnectionConfig;
import de.tuberlin.dima.dbpra.interfaces.Exercise05Interface;
import de.tuberlin.dima.dbpra.interfaces.transactions.Lineitem;
import de.tuberlin.dima.dbpra.interfaces.transactions.Order;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.DecimalFormat;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Exercise052Test {

    public static final String checkPart3 =
            "SELECT SUPPKEY, SUPPLYCOST, AVAILQTY FROM PARTSUPP ps, PART p WHERE " +
                    "p.NAME = 'brown blue puff midnight black' AND " +
                    "ps.SUPPLYCOST = ( " +
                    "SELECT MIN(SUPPLYCOST) " +
                    "FROM PARTSUPP " +
                    "WHERE PARTKEY = p.PARTKEY AND " +
                    "PARTSUPP.AVAILQTY >= 10) AND " +
                    "ps.PARTKEY = p.PARTKEY AND " +
                    "AVAILQTY >= 10 " +
                    "ORDER BY AVAILQTY ASC " +
                    "FETCH first 1 rows only";
    public static final String checkPart5 =
            "SELECT SUPPKEY, SUPPLYCOST, AVAILQTY FROM PARTSUPP ps, PART p WHERE " +
                    "p.NAME = 'midnight linen almond tomato plum' AND " +
                    "ps.SUPPLYCOST = ( " +
                    "SELECT MIN(SUPPLYCOST) " +
                    "FROM PARTSUPP " +
                    "WHERE PARTKEY = p.PARTKEY AND " +
                    "AVAILQTY >= 10) AND " +
                    "ps.PARTKEY = p.PARTKEY AND " +
                    "AVAILQTY >= 10 " +
                    "ORDER BY AVAILQTY ASC " +
                    "FETCH first 1 rows only";
    private static Connection con;
    private static double[] points = {0.25, 0.5, 0.5, 0.5, 0.5, 0.25}; // points for each exercise
    private static double[] scale = {0, 0, 0, 0, 0, 0}; // scaling factors for each exercise (in [0;1])
    private static int testCtr = 0; // test counter
    private static double summarizedScore = 0.0; // total points
    private final double tolerance = 0.01;  // rounding tolerance
    private final DecimalFormat format = new DecimalFormat("0.00");
    Exercise05Interface impl;

    @AfterClass
    public static void summarize() {
        // check only one supplier and null values only if amount or price passed
        if (scale[1] == 0 && scale[2] == 0) {
            System.out.println("Remove points for null check and only one supplier as inserting valid orders failed.");
            scale[4] = 0;
            scale[5] = 0;
        }
        System.out.println();
        summarizedScore = sum(points, scale);
    }

    // sum up array of doubles with scaling factors
    private static double sum(double[] points_in, double[] scale) {
        double s = 0;
        for (int i = 0; i < points_in.length; ++i)
            s += points[i] * scale[i];
        return s;
    }

    private static void printTestStart(String s) {
        System.out.println("Exercise 2." + (testCtr + 1) + ": test for " + s + " starts ...");
    }

    public static double getSummarizedTestScore() {
        return summarizedScore;
    }

    @Before
    public void initTestCase() throws Exception {
        // connect to database
        con = DriverManager.getConnection(ConnectionConfig.DB2_URL + ConnectionConfig.DB2_DB, ConnectionConfig.DB2_USER, ConnectionConfig.DB2_PW);
        con.setAutoCommit(true);

        impl = (Exercise05Interface) Class.forName("de.tuberlin.dima.dbpra.exercises.Exercise05").getDeclaredConstructor().newInstance();

        // insert values
        createExcerciseTwoData();
    }

    @Test(timeout = 5000)
    public void checkTransactionLevel() {
        testCtr = 0;
        printTestStart("transaction level");
        Statement statement = null;
        try {
            statement = con.createStatement();
            impl.editOrder(con, Exercise05Builder.createValidOrderSingle());

            // check lvl
            if (con.getTransactionIsolation() == Connection.TRANSACTION_READ_COMMITTED && !con.getAutoCommit()) {
                scale[testCtr] = 1;
                printSuccessful();
            } else {
                fail();
            }
        } catch (SQLException e) {
            System.out.println("transaction level test failed");
            fail();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    // In this test we check if the right amount gets removed from the items
    @Test(timeout = 5000)
    public void checkAmount() {
        testCtr = 1;
        printTestStart("amount");
        Statement statement = null;
        try {
            statement = con.createStatement();
            // Run exercise implementation
            impl.editOrder(con, Exercise05Builder.createValidOrder());

            ResultSet res3 = con.createStatement().executeQuery(checkPart3);
            if (res3.next()) {
                // TODO we should not hard code the amount of items here
                if (res3.getInt("SUPPKEY") != 1001 || res3.getInt("AVAILQTY") != (100000 - 10)) {
                    fail();
                }
            }

            ResultSet res5 = con.createStatement().executeQuery(checkPart5);
            if (res5.next()) {
                // TODO we should not hard code the amount of items here
                if (res5.getInt("SUPPKEY") != 1002 || res5.getInt("AVAILQTY") != (200000 - 10)) {
                    fail();
                }
            }
            scale[testCtr] = 1;
            printSuccessful();
        } catch (SQLException e) {
            System.out.println("amount test failed");
            e.printStackTrace();
            fail();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    // In this test we check if the right prices were calculated
    @Test(timeout = 5000)
    public void checkPrice() {
        testCtr = 2;
        printTestStart("price");
        Statement statement = null;
        try {
            statement = con.createStatement();

            Order order = Exercise05Builder.createValidOrder();
            Iterator<Lineitem> li = order.getLineitems();
            impl.editOrder(con, order);

            double pricePart3Supplier1001;
            double pricePart5Supplier1002;
            double discountPart3Supplier1001;
            double discountPart5Supplier1002;
            double correctOverallPrice;
            double userPrice;
            double userDiscount;
            double userOverallPrice;
            double userRunningSum = 0;
            double margin = 1.03;
            double discount = 0.0;

            pricePart3Supplier1001 = li.next().getQuantity() * 10 * margin;
            discountPart3Supplier1001 = pricePart3Supplier1001 * discount;
            pricePart5Supplier1002 = li.next().getQuantity() * 20 * margin;
            discountPart5Supplier1002 = pricePart5Supplier1002 * discount;
            correctOverallPrice = pricePart3Supplier1001 + pricePart5Supplier1002
                    - discountPart3Supplier1001 - discountPart5Supplier1002;

            ResultSet pricesRes = con.createStatement().executeQuery("SELECT EXTENDEDPRICE, DISCOUNT FROM LINEITEM WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr + " ORDER BY SUPPKEY ASC");

            // Article 3
            if (!pricesRes.next()) fail();
            userPrice = pricesRes.getDouble("EXTENDEDPRICE");
            userDiscount = pricesRes.getDouble("DISCOUNT");
            assertEquals(userPrice, pricePart3Supplier1001, tolerance);
            assertEquals(userDiscount, discountPart3Supplier1001, tolerance);
            userRunningSum += userPrice;

            // Article 5
            if (!pricesRes.next()) fail();
            userPrice = pricesRes.getDouble("EXTENDEDPRICE");
            userDiscount = pricesRes.getDouble("DISCOUNT");
            assertEquals(userPrice, pricePart5Supplier1002, tolerance);
            assertEquals(userDiscount, discountPart5Supplier1002, tolerance);
            userRunningSum += userPrice;

            // Overall order price
            ResultSet overallPriceRes = con.createStatement().executeQuery("SELECT TOTALPRICE FROM ORDERS WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);
            if (!overallPriceRes.next()) fail();
            userOverallPrice = overallPriceRes.getDouble("TOTALPRICE");
            assertEquals(userOverallPrice, userRunningSum * (1 - discount), tolerance);
            assertEquals(userOverallPrice, correctOverallPrice, tolerance);

            scale[testCtr] = 1;
            printSuccessful();
        } catch (SQLException e) {
            System.out.println("price test failed with SQLException");
            e.printStackTrace();
            fail();
        } catch (AssertionError e) {
            System.out.println("price test failed with AssertionError");
            fail();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    // In this test we check if the right prices are calculated for products with discount
    @Test(timeout = 5000)
    public void checkDiscount() {
        testCtr = 3;
        printTestStart("discount");
        Statement statement = null;
        try {
            statement = con.createStatement();

            Order order = Exercise05Builder.createOrderWithDiscount();
            Iterator<Lineitem> li = order.getLineitems();
            impl.editOrder(con, order);

            double pricePart3Supplier1001;
            double pricePart5Supplier1002;
            double discountPart3Supplier1001;
            double discountPart5Supplier1002;
            double correctOverallPrice;
            double userPrice;
            double userDiscount;
            double userOverallPrice;
            double userRunningSum = 0;
            double margin = 1.03;
            double discount = 0.06;

            pricePart3Supplier1001 = li.next().getQuantity() * 10 * margin;
            discountPart3Supplier1001 = pricePart3Supplier1001 * discount;
            pricePart5Supplier1002 = li.next().getQuantity() * 20 * margin;
            discountPart5Supplier1002 = pricePart5Supplier1002 * discount;
            correctOverallPrice = pricePart3Supplier1001 + pricePart5Supplier1002
                    - discountPart3Supplier1001 - discountPart5Supplier1002;

            ResultSet pricesRes = con.createStatement().executeQuery("SELECT EXTENDEDPRICE, DISCOUNT FROM LINEITEM WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr + " ORDER BY SUPPKEY ASC");

            // Part 3
            if (!pricesRes.next()) fail();
            userPrice = pricesRes.getDouble("EXTENDEDPRICE");
            userDiscount = pricesRes.getDouble("DISCOUNT");
            assertEquals(userPrice, pricePart3Supplier1001, tolerance);
            assertEquals(userDiscount, discountPart3Supplier1001, tolerance);
            userRunningSum += userPrice;

            // Part 5
            if (!pricesRes.next()) fail();
            userPrice = pricesRes.getDouble("EXTENDEDPRICE");
            userDiscount = pricesRes.getDouble("DISCOUNT");
            assertEquals(userPrice, pricePart5Supplier1002, tolerance);
            assertEquals(userDiscount, discountPart5Supplier1002, tolerance);
            userRunningSum += userPrice;

            // Overall order price
            ResultSet overallPriceRes = con.createStatement().executeQuery("SELECT TOTALPRICE FROM ORDERS WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);
            if (!overallPriceRes.next()) fail();
            userOverallPrice = overallPriceRes.getDouble("TOTALPRICE");
            assertEquals(userOverallPrice, userRunningSum * (1 - discount), tolerance);
            assertEquals(userOverallPrice, correctOverallPrice, tolerance);

            scale[testCtr] = 1;
            printSuccessful();
        } catch (SQLException e) {
            System.out.println("price test failed with SQLException");
            e.printStackTrace();
            fail();
        } catch (AssertionError e) {
            System.out.println("price test failed with AssertionError");
            fail();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }


    @Test(timeout = 5000)
    public void checkNullValues() {
        testCtr = 4;
        printTestStart("null values");
        Statement statement = null;
        try {
            statement = con.createStatement();

            impl.editOrder(con, Exercise05Builder.createNullOrder());

            ResultSet order = con.createStatement().executeQuery("SELECT * FROM ORDERS WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);
            if (order.next()) fail();

            ResultSet lineitem = con.createStatement().executeQuery("SELECT * FROM LINEITEM WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);
            if (lineitem.next()) fail();

            scale[testCtr] = 1;
            printSuccessful();
        } catch (SQLException e) {
            System.out.println("null values test failed");
            e.printStackTrace();
            fail();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    @Test(timeout = 5000)
    public void checkOnlyOneSupplier() {
        testCtr = 5;
        printTestStart("one supplier only");
        Statement statement = null;
        try {
            statement = con.createStatement();

            impl.editOrder(con, Exercise05Builder.createInValidOrder());

            ResultSet res3 = con.createStatement().executeQuery(checkPart3);
            if (res3.next()) {
                if (res3.getInt("SUPPKEY") != 1001) {
                    fail();
                }
            }

            ResultSet res5 = con.createStatement().executeQuery(checkPart5);
            if (res5.next()) {
                if (res5.getInt("SUPPKEY") != 1002) {
                    fail();
                }
            }

            ResultSet order = con.createStatement().executeQuery("SELECT * FROM ORDERS WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);
            if (order.next()) fail();

            ResultSet lineitem = con.createStatement().executeQuery("SELECT * FROM LINEITEM WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);
            if (lineitem.next()) fail();

            scale[testCtr] = 1;
            printSuccessful();
        } catch (SQLException e) {
            System.out.println("one supplier only test failed");
            e.printStackTrace();
            fail();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    @After
    public void cleanUp() throws Exception {
        int outCtr = testCtr + 1;
        System.out.println("+" + format.format(scale[testCtr] * points[testCtr]) + " for test " + outCtr + "\n");

        con.setAutoCommit(true);

        deleteExcerciseTwoData();

        con.close();
    }

    private void createExcerciseTwoData() throws SQLException {
        deleteExcerciseTwoData();

        con.createStatement().executeUpdate("INSERT INTO SUPPLIER (SUPPKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL) VALUES (1001,'Supplier#000001001','sep4GQHrXe',17,'27-971-649-2792',1000)");
        con.createStatement().executeUpdate("INSERT INTO PARTSUPP (PARTKEY, SUPPKEY, AVAILQTY, SUPPLYCOST) VALUES (3, 1001, 100000, 10)");
        con.createStatement().executeUpdate("INSERT INTO SUPPLIER (SUPPKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL) VALUES (1002,'Supplier#000001002','sep4GQHrXe',17,'27-971-649-2792',10000)");
        con.createStatement().executeUpdate("INSERT INTO PARTSUPP (PARTKEY, SUPPKEY, AVAILQTY, SUPPLYCOST) VALUES (5, 1002, 200000, 20)");
    }

    private void deleteExcerciseTwoData() throws SQLException {
        con.createStatement().executeUpdate("DELETE FROM LINEITEM WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);
        con.createStatement().executeUpdate("DELETE FROM ORDERS WHERE ORDERKEY = " + Exercise05Builder.initialOrderNr);

        con.createStatement().executeUpdate("DELETE FROM PARTSUPP WHERE SUPPKEY = 1001");
        con.createStatement().executeUpdate("DELETE FROM SUPPLIER WHERE SUPPKEY = 1001");
        con.createStatement().executeUpdate("DELETE FROM PARTSUPP WHERE SUPPKEY = 1002");
        con.createStatement().executeUpdate("DELETE FROM SUPPLIER WHERE SUPPKEY = 1002");
    }

    private void printSuccessful() {
        System.out.println("... and passed successfully.");
    }
}
