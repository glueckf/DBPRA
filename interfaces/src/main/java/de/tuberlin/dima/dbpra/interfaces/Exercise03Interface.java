package de.tuberlin.dima.dbpra.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

public interface Exercise03Interface {
    /**
     * Function to create the KundenKontaktDaten table. If the table already
     * exists, its contents should be deleted.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs. No(!) exception should be
     *                      thrown if the table already exists!
     */
    void initCustomerContactTable(Connection con) throws SQLException;

    /**
     * Function to delete the KundenKontaktDaten table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs. No(!) exception should be
     *                      thrown if the table already exists!
     */
    void dropCustomerContactTable(Connection con) throws SQLException;

    /**
     * Function to add a referential constraint with the name KKD_Kunden_FK.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    void addRefConstraint(Connection con) throws SQLException;

    /**
     * Function to add a check constrait TwitterID_CHECK on attribute
     * Twitter_ID in the KundenKontaktDaten table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    void addCheckTwitterConstraint(Connection con) throws SQLException;

    /**
     * Function to add a check constrait FacebookUndGoogleID_CHECK on attributes
     * Facebook_ID and Google_ID in the KundenKontaktDaten table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    void addCheckFacebookGoogleConstraint(Connection con) throws SQLException;

    /**
     * Function to add a check constrait CHECK_Telefonnummer on attribute
     * Telefonnummer in the KundenKontaktDaten table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    void addPhoneNumberConstraint(Connection con) throws SQLException;

    /**
     * Function to add a check constraint named MindestensEinKontakt_CHECK
     * to the table KundenKontaktDaten.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    void addAtLeastOneConstraint(Connection con) throws SQLException;

    /**
     * Function to add a trigger to the KundenKontaktDaten. The function should
     * first create the TelefonnummerAenderungen table (see task description).
     * If the table already exists, the contents of the table should be deleted.
     *
     * The function should then create a trigger for the Kundenkontakdaten table.
     *
     * @param con database connection object.
     * @throws SQLException If an error occurs.
     */
    void createTrigger(Connection con) throws SQLException;
}
