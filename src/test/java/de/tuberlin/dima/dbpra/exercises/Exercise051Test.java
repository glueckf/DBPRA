package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.config.ConnectionConfig;
import de.tuberlin.dima.dbpra.interfaces.Exercise05Interface;
import de.tuberlin.dima.dbpra.interfaces.transactions.PartSuppEntry;
import de.tuberlin.dima.dbpra.interfaces.transactions.Supplier;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Exercise051Test {
    static double summarizedScore = 0.0;
    private static double[] points = {0.5, 0.5, 0.5, 1};  // points for each exercise
    private static double[] scale = {0, 0, 0, 0};  // scaling factors for each exercise (in [0;1])
    private static DecimalFormat format = new DecimalFormat("0.00");
    private static int testCtr = 0;  // test counter
    private static Connection con;
    private static Statement statement;
    private static int maxSupplierId;
    private static int maxSupplyCost;
    Exercise05Interface impl;

    @AfterClass
    public static void summarize() throws Exception {
        // check only one supplier and null values only if amount or price passed
        if (scale[1] == 1 && scale[2] == 1 && scale[3] == 0) {
            System.out.println("Remove points for tests 2 and 3 as inserting a valid Lieferant failed.");
            scale[1] = 0;
            scale[2] = 0;
        }

        summarizedScore = sum(points, scale);
        System.out.print("Total " + format.format(summarizedScore) + " points ... normalizing...");

        System.out.println("");
        System.out.println("Group achieved " + format.format(sum(points, scale)) + " points.");
        System.out.println("");
    }

    public static double getSummarizedTestScore() {
        return summarizedScore;
    }

    // sum up array of doubles with scaling factors
    private static double sum(double[] points_in, double[] scale) {
        double s = 0;
        for (int i = 0; i < points_in.length; ++i) {
            s += points[i] * scale[i];
        }
        return s;
    }

    private static void printTestStart(String s) {
        System.out.println("Testing Exercise 1." + (testCtr + 1) + ": " + s);
    }

    private static Supplier createSupplier(int type) {
        Random rand = new Random();
        // new supplier
        int id = ++maxSupplierId;

        int suppcost_p1 = 0;
        int suppcost_p2 = 0;
        if (type == 1) {
            // invalid price
            suppcost_p1 = maxSupplyCost + 1;
            suppcost_p2 = maxSupplyCost + 1;
        } else if (type == 2) {
            // invalid price for 1 product
            suppcost_p1 = maxSupplyCost + 1;
        }

        List<PartSuppEntry> offer = new LinkedList<>();
        PartSuppEntry part1 = new PartSuppEntry(id, 1, rand.nextInt(400) + 50, suppcost_p1);
        offer.add(part1);
        PartSuppEntry part2 = new PartSuppEntry(id, 2, rand.nextInt(400) + 50, suppcost_p2);
        offer.add(part2);
        return new Supplier(id, "Supplier" + id, "42 Wallaby Way", rand.nextInt(25), "13-715-945-6730", offer, type);
    }

    @Before
    public void initTestCase() throws Exception {
        // connect to database
        con = DriverManager.getConnection(ConnectionConfig.DB2_URL + ConnectionConfig.DB2_DB, ConnectionConfig.DB2_USER, ConnectionConfig.DB2_PW);

        // create statement
        statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(100000);

        try {
            // the biigest suppkey till now
            statement.execute("SELECT MAX(SUPPKEY) FROM SUPPLIER");
            ResultSet set = statement.getResultSet();
            set.next();
            maxSupplierId = set.getInt(1);
            set.close();

            // the biggest supply cost till now
            statement.execute("SELECT max(SUPPLYCOST) FROM PARTSUPP");
            set = statement.getResultSet();
            set.next();
            maxSupplyCost = set.getInt(1);
            set.close();
        } catch (SQLException e) {
            System.out.println("Error by executing MAX(SUPPKEY)");
            e.printStackTrace();
        }

        // connect to database
        impl = (Exercise05Interface) Class.forName("de.tuberlin.dima.dbpra.exercises.Exercise05").getDeclaredConstructor().newInstance();
    }

    @Test(timeout = 20000)
    public void test1CheckTransactionLevel() {
        testCtr = 0;
        scale[testCtr] = 0;
        printTestStart("Testing transaction level");

        Statement statement = null;
        try {
            statement = con.createStatement();
            Supplier supplier = createSupplier(0);
            impl.insertPartSuppEntry(con, supplier);

            // check lvl
            if (con.getTransactionIsolation() != Connection.TRANSACTION_SERIALIZABLE) {
                fail("Wrong Isolation Level");
            }
            if (con.getAutoCommit()) {
                fail("Transactions are not used");
            }
            scale[testCtr] = 1;
            printSuccessful();
        } catch (SQLException e) {
            System.out.println("transaction level test failed");
            fail();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    @Test(timeout = 20000)
    public void test2CheckInvalidSupplier1() {
        testCtr = 1;
        scale[testCtr] = 0;
        printTestStart("Testing insertion of invalid Supplier entry");

        int countBeforeLieferant = 1, countAfterLieferant = 0;
        int countBeforeLiefert = 1, countAfterLiefert = 0;

        try {
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.execute("SELECT count(*) FROM SUPPLIER");
            ResultSet set = stmt.getResultSet();
            set.next();
            countBeforeLieferant = set.getInt(1);
            set.close();

            stmt.execute("SELECT count(*) FROM PARTSUPP");
            set = stmt.getResultSet();
            set.next();
            countBeforeLiefert = set.getInt(1);
            set.close();
        } catch (SQLException e) {
            System.out.println("Error executing COUNT(*)");
            fail();
        }

        //invalid Supplier
        impl.insertPartSuppEntry(con, createSupplier(1));

        try {
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.execute("SELECT count(*) FROM SUPPLIER");
            ResultSet set = stmt.getResultSet();
            set.next();
            countAfterLieferant = set.getInt(1);
            set.close();

            stmt.execute("SELECT count(*) FROM PARTSUPP");
            set = stmt.getResultSet();
            set.next();
            countAfterLiefert = set.getInt(1);
            set.close();
        } catch (SQLException e) {
            System.out.println("Error executing von COUNT(*)");
            fail();
        }

        assertEquals(countBeforeLieferant, countAfterLieferant);
        assertEquals(countBeforeLiefert, countAfterLiefert);
        scale[testCtr] = 1;
        printSuccessful();
    }

    @Test(timeout = 20000)
    public void test3CheckInvalidSpplier2() {
        testCtr = 2;
        scale[testCtr] = 0;
        printTestStart("Testing insertion of invalid Supplier entry");

        int countBeforeLieferant = 1, countAfterLieferant = 0;
        int countBeforeLiefert = 1, countAfterLiefert = 0;

        try {
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.execute("SELECT count(*) FROM SUPPLIER");
            ResultSet set = stmt.getResultSet();
            set.next();
            countBeforeLieferant = set.getInt(1);
            set.close();

            stmt.execute("SELECT count(*) FROM PARTSUPP");
            set = stmt.getResultSet();
            set.next();
            countBeforeLiefert = set.getInt(1);
            set.close();
        } catch (SQLException e) {
            System.out.println("Error executing COUNT(*)");
            fail();
        }

        //invalid Supplier with one offer
        impl.insertPartSuppEntry(con, createSupplier(2));

        try {
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.execute("SELECT count(*) FROM SUPPLIER");
            ResultSet set = stmt.getResultSet();
            set.next();
            countAfterLieferant = set.getInt(1);
            set.close();

            stmt.execute("SELECT count(*) FROM PARTSUPP");
            set = stmt.getResultSet();
            set.next();
            countAfterLiefert = set.getInt(1);
            set.close();
        } catch (SQLException e) {
            System.out.println("Error executing COUNT(*)");
            fail();
        }

        assertEquals(countBeforeLieferant, countAfterLieferant);
        assertEquals(countBeforeLiefert, countAfterLiefert);
        scale[testCtr] = 1;
        printSuccessful();
    }

    @Test(timeout = 20000)
    public void test4CheckValidSupplier() {
        testCtr = 3;
        scale[testCtr] = 0;
        printTestStart("Testing insertion of valid Supplier entry");

        int countBeforeLieferant = 1, countAfterLieferant = 0;
        int countBeforeLiefert = 1, countAfterLiefert = 0;

        try {
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.execute("SELECT count(*) FROM SUPPLIER");
            ResultSet set = stmt.getResultSet();
            set.next();
            countBeforeLieferant = set.getInt(1);
            set.close();

            stmt.execute("SELECT count(*) FROM PARTSUPP");
            set = stmt.getResultSet();
            set.next();
            countBeforeLiefert = set.getInt(1);
            set.close();
        } catch (SQLException e) {
            System.out.println("Error executing COUNT(*)");
            fail();
        }

        //valid Supplier
        Supplier supplier = createSupplier(0);
        impl.insertPartSuppEntry(con, supplier);

        try {
            Statement stmt = con.createStatement();
            stmt.execute("SELECT count(*) FROM SUPPLIER");
            ResultSet set = stmt.getResultSet();
            set.next();
            countAfterLieferant = set.getInt(1);
            set.close();

            stmt.execute("SELECT count(*) FROM PARTSUPP");
            set = stmt.getResultSet();
            set.next();
            countAfterLiefert = set.getInt(1);
            set.close();
        } catch (SQLException e) {
            System.out.println("Error executing COUNT(*)");
            fail();
        }
        assertEquals(countBeforeLieferant + 1, countAfterLieferant);
        assertEquals(countBeforeLiefert + supplier.partSuppEntries.size(), countAfterLiefert);
        scale[testCtr] = 1;
        printSuccessful();
    }

    @After
    public void cleanUp() throws Exception {
        int outCtr = testCtr + 1;
        System.out.println("+" + format.format(scale[testCtr] * points[testCtr]) + " for test " + outCtr + "\n");

        con.setAutoCommit(true);

        // remove newly added entries in case something weird happened
        try {
            con.createStatement().executeUpdate("DELETE FROM PARTSUPP WHERE SUPPKEY>" + 1000);
        } catch (SQLException ignored) {
        }
        try {
            con.createStatement().executeUpdate("DELETE FROM SUPPLIER WHERE SUPPKEY>" + 1000);
        } catch (SQLException ignored) {
        }

        con.close();
    }

    private void printSuccessful() {
        System.out.println("... and passed successfully.");
    }
}
