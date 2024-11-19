package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.config.ConnectionConfig;
import de.tuberlin.dima.dbpra.interfaces.Exercise03Interface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;


public class Exercise03Test extends Exercise03 {
    private Connection con;
    private Exercise03Interface testInstance;

    @Before
    public void setUp() throws Exception {
        // connect to database
        con = DriverManager.getConnection("jdbc:db2://gnu.dima.tu-berlin.de:50000/" + ConnectionConfig.DB2_DB, ConnectionConfig.DB2_USER, ConnectionConfig.DB2_PW);

        // Ensure we have a clean database.
        testInstance = (Exercise03Interface) Class.forName("de.tuberlin.dima.dbpra.exercises.Exercise03").getDeclaredConstructor().newInstance();
        cleanTables();
    }

    // Clear all required tables.
    private void cleanTables() {
        try {
            con.createStatement().executeUpdate("DROP TABLE CustomerContactData");
        } catch (SQLException e) {
            // Do nothing.
        }
        try {
            con.createStatement().executeUpdate("DROP TABLE PhoneChanges");
        } catch (SQLException e) {
            // Do nothing.
        }
        // Remove the dummy entries from Customer.
        try {
            con.createStatement().executeUpdate("DELETE FROM Customer WHERE Custkey=15001");
        } catch (SQLException e) {
            // Do nothing.
        }
    }

    @After
    public void cleanUp() throws Exception {
        // Drop the tables.
        cleanTables();
        con.close();
    }

    @Test
    public void TestInitTable() throws SQLException {
        try {
            testInstance.initCustomerContactTable(con);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            // Should not happen.
            fail();
        }
        // Make sure the table was correctly created.
        Statement stmt = con.createStatement();
        try {
            ResultSet rs = stmt.executeQuery("SELECT * FROM CustomerContactData");
            // Check the column definitions.
            assertEquals(9, rs.getMetaData().getColumnCount());
            // Check the column names.
            assertEquals("custkey", rs.getMetaData().getColumnName(1).toLowerCase());
            assertEquals("twitterid", rs.getMetaData().getColumnName(2).toLowerCase());
            assertEquals("googleid", rs.getMetaData().getColumnName(3).toLowerCase());
            assertEquals("facebookid", rs.getMetaData().getColumnName(4).toLowerCase());
            assertEquals("instagramid", rs.getMetaData().getColumnName(5).toLowerCase());
            assertEquals("githubid", rs.getMetaData().getColumnName(6).toLowerCase());
            assertEquals("telegramname", rs.getMetaData().getColumnName(7).toLowerCase());
            assertEquals("zipcode", rs.getMetaData().getColumnName(8).toLowerCase());
            assertEquals("phone", rs.getMetaData().getColumnName(9).toLowerCase());
            // Check the column types.
            assertEquals(java.sql.Types.INTEGER, rs.getMetaData().getColumnType(1));
            assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(2));
            assertEquals(35, rs.getMetaData().getColumnDisplaySize(2));
            assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(3));
            assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(4));
            assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(5));
            assertEquals(26, rs.getMetaData().getColumnDisplaySize(5));
            assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(6));
            assertEquals(34, rs.getMetaData().getColumnDisplaySize(6));
            assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(7));
            assertEquals(38, rs.getMetaData().getColumnDisplaySize(7));
            assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(8));
            assertEquals(5, rs.getMetaData().getColumnDisplaySize(8));
            assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(9));
            assertEquals(40, rs.getMetaData().getColumnDisplaySize(9));
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            fail("initCustomerContactTable failed: can't create table"); // Should not happen.
        }
        // Check the NULL conditions.
        try {
            stmt.executeUpdate("INSERT INTO CustomerContactData VALUES(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            fail("initCustomerContactTable failed: -> can't add values");
        }
        try {
            stmt.executeUpdate("INSERT INTO CustomerContactData VALUES(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            fail("initCustomerContactTable failed: -> no primary key");
        } catch (SQLException e) {
            assertEquals(-407, e.getErrorCode());
        }
        // Now check the primary key condition.
        try {
            stmt.executeUpdate("INSERT INTO CustomerContactData VALUES(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            fail("initCustomerContactTable failed: -> no primary key");
        } catch (SQLException e) {
            assertEquals(-803, e.getErrorCode());
        }
        System.out.println("initKundenKontaktTable passed.");
    }

    @Test
    public void TestReInitTable() throws SQLException {
        try {
            testInstance.initCustomerContactTable(con);
            testInstance.initCustomerContactTable(con);
            // Insert a few values.
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(20, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            // Now ensure that calling init table deletes all rows.
            testInstance.initCustomerContactTable(con);
            ResultSet rs = con.createStatement().executeQuery("SELECT * FROM CustomerContactData");
            assertFalse(rs.next());    // Should be empty.
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            // Should not happen.
            fail("In initCustomerContactTable: if the table already exists, the content should be deleted instead.");
        }
    }

    @Test
    public void TestDropTable() throws SQLException {
        try {
            testInstance.initCustomerContactTable(con);
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            testInstance.dropCustomerContactTable(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            fail("dropCustomerContactTable failed.");
        }
        try {
            con.createStatement().executeQuery("SELECT * FROM CustomerContactData");
            fail("dropCustomerContactTable failed: table still exists.");
        } catch (SQLException e) {
            assertEquals(-204, e.getErrorCode());
        }
        try {
            testInstance.dropCustomerContactTable(con);
            testInstance.dropCustomerContactTable(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            fail("dropCustomerContactTable failed.");
        }
        System.out.println("dropCustomerContactTable passed.");
    }

    @Test
    public void TestRefConstraint() throws SQLException {
        // Construct the table.
        testInstance.initCustomerContactTable(con);
        // Add the referential constraint.
        try {
            testInstance.addRefConstraint(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            fail("addRefConstraint failed.");    // Should not happen.
        }
        // Make sure the referential constraint has the correct name.
        try {
            ResultSet rs = con.createStatement().executeQuery("SELECT * FROM " +
                    "syscat.tabconst WHERE tabname='CUSTOMERCONTACTDATA' " +
                    "AND tabschema='" +
                    ConnectionConfig.DB2_USER.toUpperCase() + "' " +
                    "AND constname='CCD_CUSTOMER_FK'");
            assertTrue(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            fail("addRefConstraint failed: wrong constraint name.");
        }

        // Now check the referential constraint. First, we try adding a
        // customer that does not exist in Customer.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(15001, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            fail("addRefConstraint failed: can add invalid values");
        } catch (SQLException e) {
            assertEquals(-530, e.getErrorCode());
        }
        // Now insert a new customer to the Customer database ...
        con.createStatement().executeUpdate("INSERT INTO Customer VALUES(15001, " +
                "'Fake McFakinson', 'Fakest. 1234', 15, '12-345-678-910', 200, " +
                "'BUILDING')");
        // ... and try re-inserting the value.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(15001, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            fail("addRefConstraint failed: can't add valid values");
        }
        // Try to update the ID of the referenced customer.
        try {
            con.createStatement().executeUpdate("UPDATE Customer SET " +
                    "Custkey=15002 WHERE Custkey=15001");
            fail("addRefConstraint failed: can update to invalid value");
        } catch (SQLException e) {
            assertEquals(-531, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("UPDATE CustomerContactData SET " +
                    "Custkey=15002 WHERE Custkey=15001");
            fail("addRefConstraint failed: can update to invalid value");
        } catch (SQLException e) {
            assertEquals(-530, e.getErrorCode());
        }
        // Now make sure the deletes are cascaded.
        try {
            con.createStatement().executeUpdate("DELETE FROM Customer WHERE " +
                    "Custkey=15001");
            ResultSet rs = con.createStatement().executeQuery("SELECT * FROM CustomerContactData");
            assertFalse(rs.next());
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            fail("addRefConstraint failed: no cascade.");
        }
        System.out.println("addRefConstraint passed.");
    }

    @Test
    public void TestAtLeastOneConstraint() throws SQLException {
        // Construct the table.
        testInstance.initCustomerContactTable(con);
        try {
            testInstance.addAtLeastOneConstraint(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.out.println("addAtLeastOneConstraint failed.");
            fail();
        }
        // Check the name of the constraint.
        try {
            ResultSet rs = con.createStatement().executeQuery("SELECT * FROM " +
                    "syscat.tabconst WHERE tabname='CUSTOMERCONTACTDATA' " +
                    "AND tabschema='" +
                    ConnectionConfig.DB2_USER.toUpperCase() + "' " +
                    "AND constname='ATLEASTONE_CONSTRAINT'");
            assertTrue(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("addAtLeastOneConstraint failed.");
            fail();
        }
        // Try an invalid value.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addAtLeastOneConstraint failed.");
            fail(); // Trigger should have prevented this.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }

        // Try a few valid values.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, 'Twitter_id', NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(2, NULL, 12345, NULL, NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(3, NULL, NULL, 54321, NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(4, NULL, NULL, NULL, NULL, NULL, NULL, '12345', NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '+49 176 09812355')");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(6, 'Twitter_id', NULL, NULL, 'Ig_id', NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(7, 'Twitter_id', NULL, 525, NULL, 'Github_id', NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(8, NULL, NULL, NULL, NULL, NULL, NULL, '12345', '+49 176 09812355')");

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("addAtLeastOneConstraint failed.");
            fail();
        }
        System.out.println("addAtLeastOneConstraint passed.");
    }

    @Test
    public void TestCheckFacebookGoogleConstraint() throws SQLException {
        // Construct the table.
        testInstance.initCustomerContactTable(con);
        // Add the check twitter constraint.
        try {
            testInstance.addCheckFacebookGoogleConstraint(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            fail("addCheckFacebookGoogleConstraint failed.");
        }
        // Check the name of the constraint.
        try {
            ResultSet rs = con.createStatement().executeQuery("SELECT * FROM " +
                    "syscat.tabconst WHERE tabname='CUSTOMERCONTACTDATA' " +
                    "AND tabschema='" +
                    ConnectionConfig.DB2_USER.toUpperCase() + "' " +
                    "AND constname='FACEBOOKANDGOOGLEID_CHECK'");
            assertTrue(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            fail("addCheckFacebookGoogleConstraint failed.");
        }
        // Try to insert a few invalid values.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, " +
                    "99999, " +
                    "NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckFacebookGoogleConstraint failed.");
            fail("Google ID is too small."); // Google ID is too small.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, NULL, " +
                    "100000, " +
                    "NULL, NULL, NULL, NULL, NULL)");
            fail("addCheckFacebookGoogleConstraint failed: Facebook ID is too small."); // Facebook ID is too small.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, " +
                    "1000000, " +
                    "NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckFacebookGoogleConstraint failed.");
            fail("Google ID is too large."); // Google ID is too large.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, NULL, " +
                    "1000000, " +
                    "NULL, NULL, NULL, NULL, NULL)");
            fail("addCheckFacebookGoogleConstraint failed: Facebook ID is too large."); // Facebook ID is too large.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, " +
                    "886345, " +
                    "NULL, NULL, NULL, NULL, NULL, NULL)");
            fail("Blacklisted GoogleID."); // Blacklisted GoogleID.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, " +
                    "456291, " +
                    "NULL, NULL, NULL, NULL, NULL, NULL)");
            fail("Blacklisted GoogleID."); // Blacklisted GoogleID.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, " +
                    "366667, " +
                    "NULL, NULL, NULL, NULL, NULL, NULL)");
            fail("addCheckFacebookGoogleConstraint failed:Blacklisted GoogleID."); // Blacklisted GoogleID.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, " +
                    "227456, " +
                    "NULL, NULL, NULL, NULL, NULL, NULL)");
            fail("addCheckFacebookGoogleConstraint failed: Blacklisted GoogleID."); // Blacklisted GoogleID.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, NULL, " +
                    "623812, " +
                    "NULL, NULL, NULL, NULL, NULL)");
            fail("addCheckFacebookGoogleConstraint failed: Blacklisted FacebookID"); // Blacklisted FacebookID
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, NULL, " +
                    "736748, " +
                    "NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckFacebookGoogleConstraint failed.");
            fail("Blacklisted FacebookID"); // Blacklisted FacebookID
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, NULL, " +
                    "698222, " +
                    "NULL, NULL, NULL, NULL, NULL)");
            fail("addCheckFacebookGoogleConstraint failed: Blacklisted FacebookID"); // Blacklisted FacebookID
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, NULL, " +
                    "981372, " +
                    "NULL, NULL, NULL, NULL, NULL)");
            fail("addCheckFacebookGoogleConstraint failed: Blacklisted FacebookID"); // Blacklisted FacebookID
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        // Now try some valid values.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, NULL, " +
                    "999988, " +
                    "NULL, NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(2, NULL, " +
                    "NULL, 100234, " +
                    "NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(3, NULL, " +
                    "100042, 100011, " +
                    "NULL, NULL, NULL, NULL, NULL)");
        } catch (SQLException e) {
            e.printStackTrace();
            fail("addCheckFacebookGoogleConstraint failed: can't add valid values.");
        }
        System.out.println("addCheckFacebookGoogleConstraint passed.");
    }

    @Test
    public void TestCheckPhoneNumberConstraint() throws SQLException {
        // Construct the table.
        testInstance.initCustomerContactTable(con);
        // Add the check twitter constraint.
        try {
            testInstance.addPhoneNumberConstraint(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.out.println("addPhoneNumberConstraint failed.");
            fail();
        }
        // Check the name of the constraint.
        try {
            ResultSet rs = con.createStatement().executeQuery("SELECT * FROM " +
                    "syscat.tabconst WHERE tabname='CUSTOMERCONTACTDATA' " +
                    "AND tabschema='" +
                    ConnectionConfig.DB2_USER.toUpperCase() + "' " +
                    "AND constname='CHECK_PHONE'");
            assertTrue(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("addPhoneNumberConstraint failed.");
            fail();
        }
        // Try to insert a few invalid values.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+50 176 09812345')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Wrong countrycode."); // Wrong countrycode.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 139 09812345')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Area code out of range"); // Area code out of range
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 1501 09812345')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Area code out of range"); // Area code out of range
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 160 09812345')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Area code out of range"); // Area code out of range
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 150 1234567')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Phone number too small"); // Phone number too small
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 150 12345678901')");
            fail("Phone number too large"); // Phone number too large
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45-150-0123456789')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Wrong delimiter."); // Wrong delimiter.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 150-0123456789')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Wrong delimiter."); // Wrong delimiter.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 160 01g3456789')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Non-numeric character."); // Non-numeric character.
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == -420 || e.getErrorCode() == -545);
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 15g 0153456789')");
            System.out.println("addPhoneNumberConstraint failed.");
            fail("Non-numeric character."); // Non-numeric character.
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == -420 || e.getErrorCode() == -545);
        }
        // Try a few correct values:
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(1, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+45 176 09812355')");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(2, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+46 151 012345678')");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(3, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+47 147 743456789')");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData " +
                    "VALUES(4, NULL, NULL, NULL, NULL," +
                    "NULL, NULL, NULL, '+47 192 09812355')");
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Can't add valid values.");
        }
        System.out.println("addPhoneNumberConstraint passed.");
    }

    @Test
    public void TestCheckTwitterConstraint() throws SQLException {
        // Construct the table.
        testInstance.initCustomerContactTable(con);
        // Add the check twitter constraint.
        try {
            testInstance.addCheckTwitterConstraint(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.out.println("addCheckTwitterConstraint failed.");
            fail();
        }
        // Check the name of the constraint.
        try {
            ResultSet rs = con.createStatement().executeQuery("SELECT * FROM " +
                    "syscat.tabconst WHERE tabname='CUSTOMERCONTACTDATA' " +
                    "AND tabschema='" +
                    ConnectionConfig.DB2_USER.toUpperCase() + "' " +
                    "AND constname='TWITTERID_CHECK'");
            assertTrue(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("addCheckTwitterConstraint failed.");
            System.out.println("-> wrong constraint name");
            fail();
        }
        // Try to insert a few invalid values.
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, " +
                    "'A', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckTwitterConstraint failed.");
            fail("Does not start with @"); // Does not start with @
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, " +
                    "'@A', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckTwitterConstraint failed.");
            fail("Contains an uppercase letter."); // Contains an uppercase letter.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, " +
                    "'@!', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckTwitterConstraint failed.");
            fail("Contains an invalid symbol."); // Contains an invalid symbol.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, " +
                    "'@/', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckTwitterConstraint failed.");
            fail("Contains an invalid symbol."); // Contains an invalid symbol.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, " +
                    "'@aaaaaaaaaaaaaaaaaaaaaaa', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            System.out.println("addCheckTwitterConstraint failed.");
            fail("Is too long."); // Is too long.
        } catch (SQLException e) {
            assertEquals(-545, e.getErrorCode());
        }
        // Now try a valid ID:
        try {
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(1, " +
                    "'@aya1234aaaa-7', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(2, " +
                    "'@haaaaaaazzaaaaaaaaaaaa', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            con.createStatement().executeUpdate("INSERT INTO CustomerContactData VALUES(3, " +
                    "'@aya1234a*a-a7', " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("addCheckTwitterConstraint failed.");
            fail("Can't add valid values.");
        }
        System.out.println("addCheckTwitterConstraint passed.");
    }


    @Test
    public void TestTrigger() throws SQLException {
        // Construct the table.
        testInstance.initCustomerContactTable(con);
        con.createStatement().executeUpdate("INSERT INTO " +
                "CustomerContactData VALUES(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0123')");
        // Add the check triggers.
        try {
            testInstance.createTrigger(con);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.out.println("createTriggerfailed. (createTrigger returns error code " + e.getErrorCode() + ")");
            fail();
        }
        // Make sure the table exists.
        try {
            con.createStatement().executeQuery("SELECT * FROM PhoneChanges");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.out.println("createTrigger failed. (Phonehanges table does not exist)");
            fail();
        }

        // Ensure that the update log table is filled.
        try {
            con.createStatement().executeUpdate("Update CustomerContactData " +
                    "SET Phone='123' WHERE Custkey=1");
            //System.out.println("executed update");
            // This update should have created a log entry.
            ResultSet rs = con.createStatement().executeQuery("SELECT " +
                    "Custkey, OldPhone FROM PhoneChanges " +
                    "ORDER BY ChangeDate DESC");
            // This is log entry number 1
            String msg = "A log entry is missing or wrong.";
            assertTrue(msg, rs.next());
            assertEquals(msg, 1, rs.getInt(1));
            assertEquals(msg, "0123", rs.getString(2));
        } catch (SQLException e) {
            //System.out.println("1st update failed");
            System.err.println(e.getMessage());
            System.out.println("createTrigger failed.");
            fail();
        }
        // Now make sure that updates that are too quick are rejected.
        try {
            con.createStatement().executeUpdate("Update CustomerContactData " +
                    "SET Phone='1234' WHERE Custkey=1");
            System.out.println("createTrigger failed. (A quick update was not rejected.)");
            fail(); // Too quick
        } catch (SQLException e) {
            assertEquals(-438, e.getErrorCode());
            assertEquals("70001", e.getSQLState());
        }
        //System.out.println("7");
        // Wait 13 seconds, then try again.
        try {
            Thread.sleep(15 * 1000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            con.createStatement().executeUpdate("Update CustomerContactData " +
                    "SET Phone='1234' WHERE Custkey=1");
            // This update should have created a log entry.
            ResultSet rs = con.createStatement().executeQuery("SELECT " +
                    "Custkey, OldPhone FROM PhoneChanges " +
                    "ORDER BY ChangeDate DESC");
            // This is log entry Nr 2
            String msg = "A log entry is missing or wrong.";
            assertTrue(msg, rs.next());
            assertEquals(msg, 1, rs.getInt(1));
            assertEquals(msg, "123", rs.getString(2));
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("createTrigger failed.");
            fail();
        }
        // Ensure that an update that does not change anything
        // does not lead to an error.
        try {
            con.createStatement().executeUpdate("Update CustomerContactData " +
                    "SET Phone='1234' WHERE Custkey=1");
            // This update should not have created a log entry.
            ResultSet rs = con.createStatement().executeQuery("SELECT " +
                    "count(*) FROM PhoneChanges");
            assertTrue(rs.next());
            String msg = "A log entry was created erroneously.";
            assertEquals(msg, 2, rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("createTrigger failed.");
            fail();
        }
        //System.out.println("8");

        // Ensure that an update that does not change anything is not added
        // to the log.
        try {
            Thread.sleep(15 * 1000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            con.createStatement().executeUpdate("Update CustomerContactData " +
                    "SET Phone='1234' WHERE Custkey=1");
            // This update should not have created a log entry.
            ResultSet rs = con.createStatement().executeQuery("SELECT " +
                    "count(*) FROM PhoneChanges");
            assertTrue(rs.next());
            String msg = "A log entry was created although there were no changes.";
            assertEquals(msg, 2, rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("createTrigger failed.");
            fail();
        }
        System.out.println("createTrigger passed.");
    }

}
