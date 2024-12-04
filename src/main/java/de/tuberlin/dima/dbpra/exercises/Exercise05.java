package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.interfaces.Exercise05Interface;
import de.tuberlin.dima.dbpra.interfaces.transactions.Lineitem;
import de.tuberlin.dima.dbpra.interfaces.transactions.Order;
import de.tuberlin.dima.dbpra.interfaces.transactions.PartSuppEntry;
import de.tuberlin.dima.dbpra.interfaces.transactions.Supplier;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
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


    public PreparedStatement setOrderStatement(Connection connection, Order order) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(4));
        pstmt.setInt(1, order.getOrderkey());
        pstmt.setInt(2, order.getCustkey());
        pstmt.setString(3, order.getOrderstatus());
        pstmt.setDouble(4, 0);
        pstmt.setDate(5, new Date(System.currentTimeMillis()));
        pstmt.setString(6, order.getOrderpriority());
        pstmt.setString(7, order.getClerk());
        pstmt.setInt(8, order.getShippriority());
        return pstmt;
    }

    public PreparedStatement setLineItemStatement (Connection connection, Order order,  Lineitem lineitem, int suppkey, double[] price) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(getQueryString(6));
        stmt.setInt(1, order.getOrderkey());
        stmt.setInt(2, lineitem.getPartkey());
        stmt.setInt(3, suppkey);
        stmt.setDouble(4, lineitem.getLinenumber());
        stmt.setDouble(5, lineitem.getQuantity());
        stmt.setDouble(6, price[0]);
        stmt.setDouble(7, price[1]);
        stmt.setDouble(8, 0);
        stmt.setString(9, lineitem.getReturnflag());
        stmt.setString(10, lineitem.getLinestatus());
        stmt.setDate(11, Date.valueOf(lineitem.getShipdate()));
        stmt.setDate(12, Date.valueOf(lineitem.getCommitdate()));
        stmt.setDate(13, Date.valueOf(lineitem.getReceiptdate()));
        stmt.setString(14, lineitem.getShipinstruct());
        stmt.setString(15, lineitem.getShipmodet());
        return stmt;
    }

    public PreparedStatement setSupplierStatement (Connection connection, Lineitem lineitem, int suppkey) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(getQueryString(7));
        stmt.setInt(1, lineitem.getQuantity());
        stmt.setInt(2, lineitem.getPartkey());
        stmt.setInt(3, suppkey);
        return stmt;

    }

    public PreparedStatement updateOrderStatement(Connection connection, Order order, double price) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(getQueryString(8));
        stmt.setDouble(1, price);
        stmt.setInt(2, order.getOrderkey());
        return stmt;
    }

    public PreparedStatement findSuitableSupplierStatement(Connection connection, Lineitem lineitem) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(5));
        pstmt.setInt(1, lineitem.getPartkey());
        pstmt.setInt(2, lineitem.getQuantity());
        return pstmt;
    }

    public double[] calculatePrice(Lineitem lineitem, double supplycost, boolean discount) {
        double[] prices = new double[2];
        double price = lineitem.getQuantity() * supplycost * 1.03;  // Add 3% profit margin
        prices[0] = price;
        if(discount) {
            prices[1] = price * 0.06;
        } else {
            prices[1] = 0;
        }
        return prices;
    }

    // Helper method to safely close resources
    private void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                // Log the error but don't throw it
                System.err.println("Error closing resource: " + e.getMessage());
            }
        }
    }

    /**
     * Task 2 (see slides)
     */
    public void editOrder(Connection connection, Order order) {
        // Declare resources outside try block so we can close them in finally
        PreparedStatement orderStmt = null;
        PreparedStatement supplierStmt = null;
        PreparedStatement lineItemStmt = null;
        PreparedStatement updateSupplierStmt = null;
        PreparedStatement updateOrderStmt = null;
        ResultSet rs = null;

        try {
            // Begin transaction with READ_COMMITTED isolation level
            // This level prevents dirty reads while allowing better concurrency than SERIALIZABLE
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            // First pass: Validate all line items and calculate total quantity
            // Store iterator just once to avoid concurrent modification issues
            Iterator<Lineitem> lineItemIterator = order.getLineitems();
            List<Lineitem> validItems = new ArrayList<>();
            int totalQuantity = 0;

            // Process each line item once for validation
            while (lineItemIterator.hasNext()) {
                Lineitem item = lineItemIterator.next();
                if (item == null) {
                    connection.rollback();
                    return;
                }
                validItems.add(item);
                totalQuantity += item.getQuantity();
            }

            // Determine if discount applies (6% for orders of 100+ items)
            boolean applyDiscount = totalQuantity >= 100;

            // Insert the order first (we need the orderkey for line items)
            orderStmt = setOrderStatement(connection, order);
            orderStmt.executeUpdate();

            // Track total price for the entire order
            double orderTotalPrice = 0.0;

            // Process each validated line item
            for (Lineitem item : validItems) {
                // Find supplier with sufficient quantity and lowest cost
                supplierStmt = findSuitableSupplierStatement(connection, item);
                rs = supplierStmt.executeQuery();

                // Get supplier information
                int supplierKey;
                double supplyCost;
                if (rs.next()) {
                    supplierKey = rs.getInt("suppkey");
                    supplyCost = rs.getDouble("supplycost");
                    if (supplierKey == 0) {
                        connection.rollback();
                        System.out.println("No valid supplier found for part " + item.getPartkey());
                        return;
                    }
                } else {
                    connection.rollback();
                    System.out.println("No supplier with sufficient quantity for part " + item.getPartkey());
                    return;
                }

                // Calculate prices including profit margin and possible discount
                double[] prices = calculatePrice(item, supplyCost, applyDiscount);

                // Insert the line item
                lineItemStmt = setLineItemStatement(connection, order, item, supplierKey, prices);
                lineItemStmt.executeUpdate();

                // Update the running total (price minus discount)
                orderTotalPrice += (prices[0] - prices[1]);

                // Update supplier's available quantity
                updateSupplierStmt = setSupplierStatement(connection, item, supplierKey);
                updateSupplierStmt.executeUpdate();

                // Close resources for this iteration
                closeQuietly(rs);
                closeQuietly(supplierStmt);
                closeQuietly(lineItemStmt);
                closeQuietly(updateSupplierStmt);
            }

            // Update the order with final total price
            updateOrderStmt = updateOrderStatement(connection, order, orderTotalPrice);
            updateOrderStmt.executeUpdate();

            // Commit the transaction
            connection.commit();

        } catch (SQLException e) {
            // Attempt rollback on any SQL exception
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException rollbackEx) {
                // Combine both exceptions in the error message
                throw new RuntimeException("Transaction failed and rollback failed: " +
                        e.getMessage() + " & " + rollbackEx.getMessage());
            }
            throw new RuntimeException("Transaction failed: " + e.getMessage());
        } finally {
            // Close all resources in finally block
            closeQuietly(rs);
            closeQuietly(orderStmt);
            closeQuietly(supplierStmt);
            closeQuietly(lineItemStmt);
            closeQuietly(updateSupplierStmt);
            closeQuietly(updateOrderStmt);
        }
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
