package de.tuberlin.dima.dbpra.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Diese Klasse enthält Verbindungsdaten mit deiner Datenbank.
 * <p/>
 * WICHTIG!
 * Das Passwort ist natürlich NICHT Teil der Abgabe und sollte vorher gelöscht
 * werden, bzw. auf "" gesetzt werden.
 */
public class ConnectionConfig {

    // User Name für die Verbindung mit deiner Datenbank
    public final static String DB2_USER = "grp33";
    // Passwort für die Verbindung mit deiner Datenbank
    public final static String DB2_PW = "jtc8rtq2zjd4txn*TRG";
    // Name deiner Datenbank
    public final static String DB2_DB = "DBPRA";

    public static final Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:db2://gnu.dima.tu-berlin.de:50000/" + DB2_DB, DB2_USER, DB2_PW);
    }
}
