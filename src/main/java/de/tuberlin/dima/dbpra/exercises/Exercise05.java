package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.interfaces.Exercise05Interface;
import de.tuberlin.dima.dbpra.interfaces.transactions.Order;
import de.tuberlin.dima.dbpra.interfaces.transactions.PartSuppEntry;
import de.tuberlin.dima.dbpra.interfaces.transactions.Supplier;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Exercise05 implements Exercise05Interface {
    /**
     * Task 1 (see slides)
     */
    public static final Logger log = Logger.getLogger(Exercise05.class.getName());

    public PreparedStatement setPartSuppEntry(Connection connection, PartSuppEntry partSuppEntry) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(3));
        pstmt.setInt(1, partSuppEntry.getPartkey());
        pstmt.setInt(2, partSuppEntry.getSuppkey());
        pstmt.setInt(3, partSuppEntry.getAvalqty());
        pstmt.setDouble(4, partSuppEntry.getSupplycost());
        return pstmt;
    }

    public PreparedStatement setSupplier(Supplier supplier, Connection connection, PartSuppEntry partSuppEntry) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(2));
        pstmt.setInt(1, partSuppEntry.getSuppkey());
        pstmt.setString(2, supplier.getName());
        pstmt.setString(3, supplier.getName());
        pstmt.setInt(4, supplier.getNationkey());
        pstmt.setString(5, supplier.getPhone());
        pstmt.setDouble(6, supplier.getAcctbal());

        return pstmt;
    }

    public void insertPartSuppEntry(Connection connection, Supplier supplier) {
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            // Counter and list for valid entries initialization
            int competitiveSupply = 0;
            List<PartSuppEntry> partSuppEntries = new ArrayList<>();

            // First pass: identify all competitive supplies
            for (PartSuppEntry partSuppEntry : supplier.partSuppEntries) {
                PreparedStatement pstmt = connection.prepareStatement(getQueryString(1));
                pstmt.setInt(1, partSuppEntry.getPartkey());

                ResultSet rs = pstmt.executeQuery();

                if (rs.next()) {
                    double min_supp_cost = rs.getDouble("supplycost");
                    if (min_supp_cost > partSuppEntry.getSupplycost()) {
                        partSuppEntries.add(partSuppEntry);
                        competitiveSupply++;
                    }
                }
                rs.close();
                pstmt.close();
            }

            // Only proceed if supplier has 2 or more competitive supplies
            if (competitiveSupply >= 2) {
                // Insert supplier ONCE
                PreparedStatement supplierStmt = setSupplier(supplier, connection, partSuppEntries.get(0));
                supplierStmt.executeUpdate();
                supplierStmt.close();

                // Then insert all competitive partSupp entries
                for (PartSuppEntry partSuppEntry : partSuppEntries) {
                    PreparedStatement partSuppStmt = setPartSuppEntry(connection, partSuppEntry);
                    partSuppStmt.executeUpdate();
                    partSuppStmt.close();
                }
            }

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();  // Add rollback in case of error
            } catch (SQLException rollbackEx) {
                throw new RuntimeException("Error during rollback: " + rollbackEx.getMessage(), rollbackEx);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Task 2 (see slides)
     */
    public void editOrder(Connection connection, Order order) {
        // your code goes here
    }

    private String getQueryString(int i) {
        StringBuilder query = new StringBuilder();

        try {
            String path = String.format("Query%02d.sql", i);
            InputStream is = Exercise05.class.getClassLoader().getResourceAsStream(path);
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
