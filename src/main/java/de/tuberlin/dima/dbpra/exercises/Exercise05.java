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
        pstmt.setDouble(4, -1);
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
        stmt.setDouble(5, price[1]);
        stmt.setDouble(6, price[2]);
        stmt.setDouble(7, 0);
        stmt.setString(8, lineitem.getReturnflag());
        stmt.setString(9, lineitem.getLinestatus());
        stmt.setDate(10, Date.valueOf(lineitem.getShipdate()));
        stmt.setDate(11, Date.valueOf(lineitem.getCommitdate()));
        stmt.setDate(12, Date.valueOf(lineitem.getReceiptdate()));
        stmt.setString(13, lineitem.getShipinstruct());
        stmt.setString(14, lineitem.getShipmodet());
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

    public double[] calculatePrice(Lineitem lineitem, double supplycost, boolean discount){
        double[] prices = new double[2];
        double price = lineitem.getQuantity() * supplycost;
        prices[0] = price;
        if(discount){
            prices[1] = price * 0.06;
        }
        prices[1] = 0;
        return prices;

    }

    /**
     * Task 2 (see slides)
     */
    public void editOrder(Connection connection, Order order) {
        try {
            // 1. Transaction Setup
            // Set autocommit and isolation level
            // Think: What's the minimum isolation level needed?
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);


            // 2. Initial Validation
            // Check if any lineitem is null
            // Calculate total quantity for all items
            // Determine if discount applies
            int totalQty = 0;
            double totalPrice = 0;

            while(order.getLineitems().hasNext()) {
                Lineitem lineitem = order.getLineitems().next();
                if (lineitem == null) {
                    throw new RuntimeException("Lineitem is null");
                }
                totalQty += lineitem.getQuantity();
            }

            boolean discountApplies = totalQty > 100;


            // 3. Insert Order
            // Create and execute order insertion
            // Need: orderkey for later lineitem insertions
            PreparedStatement orderStmt = setOrderStatement(connection, order);
            orderStmt.executeUpdate();



            // 4. Process Each Lineitem
            Iterator<Lineitem> lineitems = order.getLineitems();
            while (lineitems.hasNext()) {
                Lineitem lineitem = lineitems.next();

                // 4.1 Find Suitable Supplier
                // Query supplier with:
                // - Enough quantity (availqty >= needed quantity)
                // - Lowest supplycost
                // If no supplier found -> rollback
                PreparedStatement supplierStmt = findSuitableSupplierStatement(connection, lineitem);
                ResultSet rs = supplierStmt.executeQuery();
                double suppCost = 0;
                int suppkey = 0;
                if(rs.next()) {
                    suppkey = rs.getInt("suppkey");
                    if (suppkey == 0){
                        throw new SQLException();
                    }
                    suppCost = rs.getDouble("supplycost");
                }

                // 4.2 Price Calculations
                // - Base price (supplycost * quantity)
                // - Add profit margin
                // - Calculate discount if applicable
                double[] price = calculatePrice(lineitem, suppCost, discountApplies);

                // 4.3 Insert Lineitem
                // Insert with calculated values
                PreparedStatement liStmt = setLineItemStatement(connection, order, lineitem, suppkey, price);
                liStmt.executeUpdate();

                // Update total Price after discounts
                totalPrice += (price[0] - price[1]);

                // 4.4 Update Supplier
                // Reduce supplier's availqty
                PreparedStatement supStmt = setSupplierStatement(connection, lineitem, suppkey);
                supStmt.executeUpdate();
            }

            // 5. Update Order Total
            // Calculate and update final order total
            // Remember to consider discounts
            PreparedStatement oStmt = updateOrderStatement(connection, order, totalPrice);
            oStmt.executeUpdate();

            // 6. Commit Transaction
            connection.commit();

        } catch (SQLException e) {
            // 7. Error Handling
            // Rollback transaction
            try {
                connection.rollback();  // Add rollback in case of error
            } catch (SQLException rollbackEx) {
                throw new RuntimeException("Error during rollback: " + rollbackEx.getMessage(), rollbackEx);
            }
            throw new RuntimeException(e);

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
