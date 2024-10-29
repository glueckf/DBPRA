package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.config.ConnectionConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * This test only performs simple checks, such as:
 * - correct schema of the result (number + names of result fields)
 * - correct number of return lines
 * - whether some expected lines are included in the result
 *
 * Passing the tests does not mean that the requests are definitive
 * correct.
 * The full test is carried out by submitting the solution in the
 * evaluation tool
 */
public class Exercise01Test {

    private Statement statement;

    private static void checkHashSorted(ResultSet resultSet, int hash) throws SQLException {
        assertEquals("Incorrect result", hash, computeOrderAwareResultSetHash(resultSet));
    }

    private static void checkHashUnsorted(ResultSet resultSet, int hash) throws SQLException {
        assertEquals("Incorrect result", hash, computeOrderTolerantResultSetHash(resultSet));
    }

    private static int computeOrderAwareResultSetHash(ResultSet result) throws SQLException {

        int hash = 0;
        int rowCnt = 0;

        result.first();
        hash = computeRowHash(result) | rowCnt++;

        while (result.next()) {
            hash ^= (1315423911 ^ ((1315423911 << 5) + (computeRowHash(result) | rowCnt++) + (1315423911 >> 2)));
        }

        return hash;
    }

    private static int computeOrderTolerantResultSetHash(ResultSet result) throws SQLException {

        int hash = 0;

        result.first();
        hash = computeRowHash(result);

        while (result.next()) {
            hash ^= (1315423911 ^ ((1315423911 << 5) + computeRowHash(result) + (1315423911 >> 2)));
        }

        return hash;

    }

    private static int computeRowHash(ResultSet result) throws SQLException {

        int hash = 0;
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            hash ^= (1315423911 ^ ((1315423911 << 5) + result.getObject(i).hashCode() + (1315423911 >> 2)));
        }
        return hash;
    }

    @Before
    public void setUp() throws Exception {

        // connect to database
        Connection con = DriverManager.getConnection("jdbc:db2://gnu.dima.tu-berlin.de:50000/" + ConnectionConfig.DB2_DB, ConnectionConfig.DB2_USER, ConnectionConfig.DB2_PW);

        // create statement
        statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        //throw new Exception ("database not found exception");
    }

    @After
    public void cleanUp() throws Exception {
        statement.getConnection().close();
    }

    @Test
    public void getSQLQuery01Test() {
        try {
            statement.execute(Exercise01.getSQLQuery01());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"MFGR"};
            assertTrue("Task 01: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashUnsorted(result, -1753821795);
        } catch (SQLException e) {
            fail("Task 01: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery02Test() {
        try {
            statement.execute(Exercise01.getSQLQuery02());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"MKTSEGMENT"};
            assertTrue("Task 02: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashUnsorted(result, 1960892788);
        } catch (SQLException e) {
            fail("Task 02: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery03Test() {
        try {
            statement.execute(Exercise01.getSQLQuery03());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"AMOUNT"};
            assertTrue("Task 03: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashUnsorted(result, -1359675387);
        } catch (SQLException e) {
            fail("Task 03: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery04Test() {
        try {
            statement.execute(Exercise01.getSQLQuery04());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"NAME", "ACCTBAL"};
            assertTrue("Task 04: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, -562053766);
        } catch (SQLException e) {
            fail("Task 04: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery05Test() {
        try {
            statement.execute(Exercise01.getSQLQuery05());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"AMOUNT", "AVERAGE"};
            assertTrue("Task 05: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashUnsorted(result, -1072442402);
        } catch (SQLException e) {
            fail("Task 05: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery06Test() {
        try {
            statement.execute(Exercise01.getSQLQuery06());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"AMOUNT", "MIN_PRICE", "MAX_PRICE", "AVG_PRICE"};
            assertTrue("Task 06: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashUnsorted(result, -1787677549);
        } catch (SQLException e) {
            fail("Task 06: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery07Test() {
        try {
            statement.execute(Exercise01.getSQLQuery07());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"SHIPMODE", "AMOUNT", "MIN", "MAX", "AVG", "TOTAL"};
            assertTrue("Task 07: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, 510220540);
        } catch (SQLException e) {
            fail("Task 07: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery08Test() {
        try {
            statement.execute(Exercise01.getSQLQuery08());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"AMOUNT", "SHIPINSTRUCT", "SHIPMODE", "RETURNFLAG"};
            assertTrue("Task 08: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashUnsorted(result, -1092057965);
        } catch (SQLException e) {
            fail("Task 08: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery09Test() {
        try {
            statement.execute(Exercise01.getSQLQuery09());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"SHIPMODE", "RETURNFLAG", "AVG_TIME", "MAX_TIME"};
            assertTrue("Task 09: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, 2073561686);
        } catch (SQLException e) {
            fail("Task 09: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery10Test() {
        try {
            statement.execute(Exercise01.getSQLQuery10());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"NATION", "AMOUNT"};
            assertTrue("Task 10: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, -1033328912);
        } catch (SQLException e) {
            fail("Task 10: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery11Test() {
        try {
            statement.execute(Exercise01.getSQLQuery11());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"NAME", "TOTAL_VOLUME"};
            assertTrue("Task 11: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, 705835838);
        } catch (SQLException e) {
            fail("Task 11: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery12Test() {
        try {
            statement.execute(Exercise01.getSQLQuery12());
            ResultSet result = statement.getResultSet();


            String[] expectedSchema = new String[]{"AMOUNT", "NAME", "SHIPMODE"};
            assertTrue("Task 12: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, -253481369);
        } catch (SQLException e) {
            fail("Task 12: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery13Test() {
        try {
            statement.execute(Exercise01.getSQLQuery13());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"NAME"};
            assertTrue("Task 13: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, -1264993842);
        } catch (SQLException e) {
            fail("Task 13: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery14Test() {
        try {
            statement.execute(Exercise01.getSQLQuery14());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"CUSTOMER"};
            assertTrue("Task 14: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashUnsorted(result, -1359666466);
        } catch (SQLException e) {
            fail("Task 14: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery15Test() {
        try {
            statement.execute(Exercise01.getSQLQuery15());
            ResultSet result = statement.getResultSet();

            String[] expectedSchema = new String[]{"CUSTKEY", "NATION", "NAME", "PHONE", "LOSS"};
            assertTrue("Task 15: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));

            checkHashSorted(result, -2120824452);
        } catch (SQLException e) {
            fail("Task 15: SQL Exception: " + e.getMessage());
        }
    }


    private boolean checkResultSchema(ResultSet result, String[] expectedSchema) throws SQLException {

        ResultSetMetaData meta = result.getMetaData();

        if (meta.getColumnCount() != expectedSchema.length)
            return false;

        for (int i = 0; i < meta.getColumnCount(); i++) {
            if (!meta.getColumnLabel(i + 1).equalsIgnoreCase(expectedSchema[i]))
                return false;
        }

        return true;
    }
}
