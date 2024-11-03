package de.tuberlin.dima.dbpra.config;

import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConnectionConfigTest {

    @Test
    public void connectionTest() {
        // test that database connection can be established

        try {
            Connection conn = ConnectionConfig.getConnection();
            conn.close();
        } catch (SQLException sqlE) {
            fail("DB2 Connection could not be established:\n" + sqlE.getMessage());
        }
    }

    @Test
    public void databaseTest() {
        // test that database contains all tables
        // test that all tables have the right data expected by the query tests

        try {

            Connection conn = DriverManager.getConnection("jdbc:db2://gnu.dima.tu-berlin.de:50000/" + ConnectionConfig.DB2_DB, ConnectionConfig.DB2_USER, ConnectionConfig.DB2_PW);
            Statement statement = conn.createStatement();
            ResultSet r;

            statement.execute("SELECT COUNT(*) FROM CUSTOMER");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 15000);
            r.close();

            statement.execute("SELECT COUNT(*) FROM PART");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 20000);
            r.close();

            statement.execute("SELECT COUNT(*) FROM SUPPLIER");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 1000);
            r.close();

            statement.execute("SELECT COUNT(*) FROM PARTSUPP");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 80000);
            r.close();

            statement.execute("SELECT COUNT(*) FROM ORDERS");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 150000);
            r.close();

            statement.execute("SELECT COUNT(*) FROM LINEITEM");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 600572);
            r.close();

            statement.execute("SELECT COUNT(*) FROM NATION");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 25);
            r.close();

            statement.execute("SELECT COUNT(*) FROM REGION");
            r = statement.getResultSet();
            r.next();
            assertTrue("Data on the server is wrong. Please reinitialize the TPC-H tables.", r.getInt(1) == 5);
            r.close();

            statement.close();
            conn.close();

        } catch (SQLException sqlE) {
            sqlE.printStackTrace();
            fail();
        }
    }}
