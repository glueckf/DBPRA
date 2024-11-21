package de.tuberlin.dima.dbpra.config;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.fail;

public class ConnectionConfigTest {

    @Test
    public void connectionTest() {
        // test that database connection can be established

        try {
            Connection conn = DriverManager.getConnection(ConnectionConfig.DB2_URL + ConnectionConfig.DB2_DB, ConnectionConfig.DB2_USER, ConnectionConfig.DB2_PW);
            conn.close();
        } catch (SQLException sqlE) {
            fail("DB2 Connection could not be established:\n" + sqlE.getMessage());
        }
    }
}
