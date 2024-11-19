package de.tuberlin.dima.dbpra.config;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

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
}
