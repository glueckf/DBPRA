package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.config.ConnectionConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 *  This test only performs simple checks, such as:
 *  - correct schema of the result (number + names of result fields)
 *  - correct number of return lines
 *  - whether some expected lines are included in the result
 *
 *  Passing the tests does not mean that the requests are definitive
 *  correct.
 *  The full test is carried out by submitting the solution in the
 *  evaluation tool
 */
public class Exercise02Test {

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
    }

    @After
    public void cleanUp() throws Exception {
        statement.getConnection().close();
    }

    @Test
    public void getSQLQuery01Test() {
        try {
            statement.execute(Exercise02.getSQLQuery01());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"MKTSEGMENT", "BALANCE"};
            assertTrue("Task 06: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, -1218593356);
        } catch (SQLException e) {
            fail("Task 01: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery02Test() {
        try {
            statement.execute(Exercise02.getSQLQuery02());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"NAME", "ADDRESS", "PHONE"};
            assertTrue("Task 02: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, 266279907);
        } catch (SQLException e) {
            fail("Task 02: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery03Test() {
        try {
            statement.execute(Exercise02.getSQLQuery03());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"PARTKEY", "SUPPLYCOST", "SUPPLIER"};
            assertTrue("Task 03: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, 1932315869);
        } catch (SQLException e) {
            fail("Task 03: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery04Test() {
        try {
            statement.execute(Exercise02.getSQLQuery04());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"MFGR", "BRAND", "NAME"};
            assertTrue("Task 04: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, 969319475);
        } catch (SQLException e) {
            fail("Task 04: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery05Test() {
        try {
            statement.execute(Exercise02.getSQLQuery05());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"PRIORITY", "AMOUNT"};
            assertTrue("Task 05: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, 71574110);
        } catch (SQLException e) {
            fail("Task 05: SQL Exception: " + e.getMessage());
        }
    }
    @Test
    public void getSQLQuery06Test() {
        try {
            statement.execute(Exercise02.getSQLQuery06());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"NAME", "SUM", "AMOUNT"};
            assertTrue("Task 06: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, 650929702);
        } catch (SQLException e) {
            fail("Task 06: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery07Test() {
        try {
            statement.execute(Exercise02.getSQLQuery07());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"BRAND"};
            assertTrue("Task 07: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, -343640736);
        } catch (SQLException e) {
            fail("Task 07: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery08Test() {
        try {
            statement.execute(Exercise02.getSQLQuery08());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"SHIPMODE", "NAME", "AMOUNT"};
            assertTrue("Task 08: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, 165573357);
        } catch (SQLException e) {
            fail("Task 08: SQL Exception: " + e.getMessage());
        }
    }

    @Test
    public void getSQLQuery09Test() {
        try {
            statement.execute(Exercise02.getSQLQuery09());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"CUSTOMER", "AMOUNT", "NATION", "REGION"};
            assertTrue("Task 09: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, -783836851);
        } catch (SQLException e) {
            fail("Task 09: SQL Exception: " + e.getMessage());
        }
    }
    @Test
    public void getSQLQuery10Test() {
        try {
            statement.execute(Exercise02.getSQLQuery10());
            ResultSet result = statement.getResultSet();
            String[] expectedSchema = new String[]{"SUPPKEY", "PARTKEY", "PART_NAME", "VALUE"};
            assertTrue("Task 10: Schema incorrect. Should be: " + Arrays.toString(expectedSchema), checkResultSchema(result, expectedSchema));
            checkHashSorted(result, 1123781623);
        } catch (SQLException e) {
            fail("Task 10: SQL Exception: " + e.getMessage());

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
