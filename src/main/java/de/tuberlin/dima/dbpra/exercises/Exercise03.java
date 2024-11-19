package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.interfaces.Exercise03Interface;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Exercise03 implements Exercise03Interface {

    /**
     * Function to create the CustomerContactTable table. If the table already
     * exists, its contents should be deleted.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs. No(!) exception should be
     *                      thrown if the table already exists!
     */
    @Override
    public void initCustomerContactTable(Connection con) throws SQLException {
        // Task 1, 0.25P
        Statement statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        try {
            statement.execute(getQueryString(1));
        } catch (SQLException e) {
            //TODO: Fehlerbehandlung
            // falls Tabelle schon existiert, lösche alle Einträge
            statement.execute("DELETE FROM CustomerContactTable");
        }
    }

    /**
     * Function to delete the CustomerContactTable table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs. No(!) exception should be
     *                      thrown if the table already exists!
     */
    @Override
    public void dropCustomerContactTable(Connection con) throws SQLException {
        // Task 2, 0.25P
        executeStatement(con, getQueryString(2));
    }

    /**
     * Function to add a referential constraint with the name described in the task.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    @Override
    public void addRefConstraint(Connection con) throws SQLException {
        // Task 3, 0.5P
        executeStatement(con, getQueryString(3));
    }

    /**
     * Function to add a check constraint
     * to the table CustomerContactTable.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    @Override
    public void addAtLeastOneConstraint(Connection con) throws SQLException {
        // Task 4, 0.5P
        executeStatement(con, getQueryString(4));
    }

    /**
     * Function to add a check constrait on some attributes
     * in the CustomerContactTable table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    @Override
    public void addCheckFacebookGoogleConstraint(Connection con) throws SQLException {
        // Task 5, 0.5P
        executeStatement(con, getQueryString(5));
    }

    /**
     * Function to add a check constrait on attribute
     * Phone in the CustomerContactTable table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    @Override
    public void addPhoneNumberConstraint(Connection con) throws SQLException {
        // Task 6, 1P
        executeUpdateStatement(con, getQueryString(6));
    }

    /**
     * Function to add a check constrait on attribute
     * Twitter_ID in the CustomerContactTable table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    @Override
    public void addCheckTwitterConstraint(Connection con) throws SQLException {
        // Task 7, 1P
        executeStatement(con, getQueryString(7));
    }


    /**
     * Function to add a trigger to the CustomerContactData. The function should
     * first create the PhoneChanges table (see task description).
     * If the table already exists, the contents of the table should be deleted.
     * <p>
     * The function should then create a trigger for the CustomerContactTable table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    @Override
    public void createTrigger(Connection con) throws SQLException {
        // Task 8, 1P

    }

    private void executeStatement(Connection con, String query) throws SQLException {
        Statement statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.execute(query);
    }

    private void executeUpdateStatement(Connection con, String query) throws SQLException {
        Statement statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.executeUpdate(query);
        statement.close();
    }

    private String getQueryString(int i) {
        StringBuilder query = new StringBuilder();

        try {
            String path = String.format("SQL3Query%02d.sql", i);
            InputStream is = Exercise03.class.getClassLoader().getResourceAsStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

            String line;
            while ((line = br.readLine()) != null) {
                if (!(line.startsWith("--") || line.isEmpty())) {
                    query.append(line);
                    query.append("\n");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while reading resource for query " + i + ": ", e);
        }

        int limit = query.lastIndexOf(";") < 0 ? query.length() : query.lastIndexOf(";");
        return query.substring(0, limit);
    }

}
