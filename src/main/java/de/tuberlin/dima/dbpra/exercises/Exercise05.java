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
     *********** HILFSFUNKTION FÜR TASK 1 & 2 ***********
     */

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


    /**
     * Task 1 (see slides)
     */

    /**
     *********** HILFSFUNKTIONEN ***********
     */

    public PreparedStatement setPartSuppEntry(Connection connection, PartSuppEntry partSuppEntry) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(3));
        pstmt.setInt(1, partSuppEntry.getPartkey());    // Teil-ID
        pstmt.setInt(2, partSuppEntry.getSuppkey());    // Lieferanten-ID
        pstmt.setInt(3, partSuppEntry.getAvalqty());    // Verfügbare Menge
        pstmt.setDouble(4, partSuppEntry.getSupplycost()); // Kosten
        return pstmt;
    }

    public PreparedStatement setSupplier(Supplier supplier, Connection connection, PartSuppEntry partSuppEntry) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(2));
        pstmt.setInt(1, partSuppEntry.getSuppkey());    // Lieferanten-ID
        pstmt.setString(2, supplier.getName());         // Name
        pstmt.setString(3, supplier.getName());         // Name
        pstmt.setInt(4, supplier.getNationkey());       // Länder-ID
        pstmt.setString(5, supplier.getPhone());        // Telefon
        pstmt.setDouble(6, supplier.getAcctbal());      // Kontostand

        return pstmt;
    }


    public void insertPartSuppEntry(Connection connection, Supplier supplier) {
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            // Zähler und Liste für konkurrenzfähige Angebote
            int competitiveSupply = 0;
            List<PartSuppEntry> partSuppEntries = new ArrayList<>();

            // 1. Durchgang: Finde alle konkurrenzfähigen Angebote
            for (PartSuppEntry partSuppEntry : supplier.partSuppEntries) {
                PreparedStatement pstmt = connection.prepareStatement(getQueryString(1));
                pstmt.setInt(1, partSuppEntry.getPartkey());

                ResultSet rs = pstmt.executeQuery();

                if (rs.next()) {
                    double min_supp_cost = rs.getDouble("supplycost");
                    if (min_supp_cost > partSuppEntry.getSupplycost()) {  // Preis ist günstiger
                        partSuppEntries.add(partSuppEntry);
                        competitiveSupply++;
                    }
                }
                rs.close();
                pstmt.close();
            }

            // Nur fortfahren wenn mind. 2 konkurrenzfähige Angebote
            if (competitiveSupply >= 2) {
                // Lieferant EINMAL einfügen
                PreparedStatement supplierStmt = setSupplier(supplier, connection, partSuppEntries.get(0));
                supplierStmt.executeUpdate();
                supplierStmt.close();

                // Dann alle günstigen Teile-Beziehungen einfügen
                for (PartSuppEntry partSuppEntry : partSuppEntries) {
                    PreparedStatement partSuppStmt = setPartSuppEntry(connection, partSuppEntry);
                    partSuppStmt.executeUpdate();
                    partSuppStmt.close();
                }
            }

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();  // Rollback bei Fehlern
            } catch (SQLException rollbackEx) {
                throw new RuntimeException("Fehler beim Rollback: " + rollbackEx.getMessage(), rollbackEx);
            }
            throw new RuntimeException(e);
        }
    }


    /**
     * Task 2 (see slides)
     */

    /**
     *********** HILFSFUNKTIONEN ***********
     */

    public PreparedStatement setOrderStatement(Connection connection, Order order) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(4));
        pstmt.setInt(1, order.getOrderkey());      // Bestell-ID
        pstmt.setInt(2, order.getCustkey());       // Kunden-ID
        pstmt.setString(3, order.getOrderstatus()); // Status
        pstmt.setDouble(4, 0);                     // Initial-Preis
        pstmt.setDate(5, new Date(System.currentTimeMillis())); // Aktuelles Datum
        pstmt.setString(6, order.getOrderpriority()); // Priorität
        pstmt.setString(7, order.getClerk());      // Sachbearbeiter
        pstmt.setInt(8, order.getShippriority());  // Versand-Priorität
        return pstmt;
    }


    public PreparedStatement setLineItemStatement(Connection connection, Order order, Lineitem lineitem, int suppkey, double[] price) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(getQueryString(6));
        stmt.setInt(1, order.getOrderkey());     // Bestell-ID
        stmt.setInt(2, lineitem.getPartkey());   // Teile-ID
        stmt.setInt(3, suppkey);                 // Lieferanten-ID
        stmt.setDouble(4, lineitem.getLinenumber()); // Positionsnummer
        stmt.setDouble(5, lineitem.getQuantity());   // Menge
        stmt.setDouble(6, price[0]);             // Einzelpreis
        stmt.setDouble(7, price[1]);             // Gesamtpreis
        stmt.setDouble(8, 0);                    // Rabatt
        stmt.setString(9, lineitem.getReturnflag());
        stmt.setString(10, lineitem.getLinestatus());
        stmt.setDate(11, Date.valueOf(lineitem.getShipdate()));
        stmt.setDate(12, Date.valueOf(lineitem.getCommitdate()));
        stmt.setDate(13, Date.valueOf(lineitem.getReceiptdate()));
        stmt.setString(14, lineitem.getShipinstruct());
        stmt.setString(15, lineitem.getShipmodet());
        return stmt;
    }


    public PreparedStatement setSupplierStatement(Connection connection, Lineitem lineitem, int suppkey) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(getQueryString(7));
        stmt.setInt(1, lineitem.getQuantity());  // Abzuziehende Menge
        stmt.setInt(2, lineitem.getPartkey());   // Teil-ID
        stmt.setInt(3, suppkey);                 // Lieferanten-ID
        return stmt;
    }


    public PreparedStatement updateOrderStatement(Connection connection, Order order, double price) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(getQueryString(8));
        stmt.setDouble(1, price);               // Neuer Preis
        stmt.setInt(2, order.getOrderkey());    // Bestell-ID
        return stmt;
    }


    public PreparedStatement findSuitableSupplierStatement(Connection connection, Lineitem lineitem) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement(getQueryString(5));
        pstmt.setInt(1, lineitem.getPartkey());   // Teil-ID
        pstmt.setInt(2, lineitem.getQuantity());  // Benötigte Menge
        return pstmt;
    }


    public double[] calculatePrice(Lineitem lineitem, double supplycost, boolean discount) {
        double[] prices = new double[2];
        double price = lineitem.getQuantity() * supplycost * 1.03;  // 3% Gewinnmarge
        prices[0] = price;                    // Einzelpreis
        if(discount) {
            prices[1] = price * 0.06;         // 6% Rabatt wenn aktiviert
        } else {
            prices[1] = 0;                    // Kein Rabatt
        }
        return prices;
    }


    /**
     * Bearbeitet eine Bestellung inkl. aller Positionen in einer Transaktion
     *
     * Ablauf:
     * 1. Validiert alle Bestellpositionen und prüft Gesamtmenge für Rabatt
     * 2. Legt Bestellung an
     * 3. Für jede Position:
     *    - Sucht günstigsten Lieferanten mit ausreichend Lagerbestand
     *    - Berechnet Preise (inkl. evt. Mengenrabatt ab 100 Stück)
     *    - Fügt Position ein
     *    - Aktualisiert Lieferanten-Lagerbestand
     * 4. Aktualisiert Gesamtpreis der Bestellung
     *
     * @param connection DB-Verbindung
     * @param order Bestellung mit allen Positionen
     */
    public void editOrder(Connection connection, Order order) {

        try {
            // Transaktion mit READ_COMMITTED starten für bessere Nebenläufigkeit
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            // 1. Durchgang: Validierung und Mengenberechnung
            Iterator<Lineitem> lineItemIterator = order.getLineitems();
            List<Lineitem> validItems = new ArrayList<>();
            int totalQuantity = 0;

            while (lineItemIterator.hasNext()) {
                Lineitem item = lineItemIterator.next();
                if (item == null) {
                    connection.rollback();
                    return;
                }
                validItems.add(item);
                totalQuantity += item.getQuantity();
            }

            // Rabatt ab 100 Stück (6%)
            boolean applyDiscount = totalQuantity >= 100;

            // Bestellung anlegen
            PreparedStatement orderStmt = setOrderStatement(connection, order);
            orderStmt.executeUpdate();

            // Gesamtpreis tracken
            double orderTotalPrice = 0.0;

            // Jede Position verarbeiten
            for (Lineitem item : validItems) {
                // Passenden Lieferanten finden
                PreparedStatement supplierStmt = findSuitableSupplierStatement(connection, item);
                ResultSet rs = supplierStmt.executeQuery();

                // Lieferantendaten auslesen
                int supplierKey;
                double supplyCost;

                if (rs.next()) {
                    supplierKey = rs.getInt("suppkey");
                    supplyCost = rs.getDouble("supplycost");

                    // Kein Lieferant gefunden
                    if (supplierKey == 0) {
                        connection.rollback();
                        System.out.println("Kein Lieferant für Teil " + item.getPartkey());
                        return;
                    }
                } else {

                    // Keine ausreichende Menge
                    connection.rollback();
                    System.out.println("Keine ausreichende Menge für Teil " + item.getPartkey());
                    return;
                }

                // Preise mit Gewinnmarge und evt. Rabatt berechnen
                double[] prices = calculatePrice(item, supplyCost, applyDiscount);

                // Position einfügen
                PreparedStatement lineItemStmt = setLineItemStatement(connection, order, item, supplierKey, prices);
                lineItemStmt.executeUpdate();

                // Gesamtpreis aktualisieren (Preis minus Rabatt)
                orderTotalPrice += (prices[0] - prices[1]);

                // Lieferanten-Lagerbestand aktualisieren
                PreparedStatement updateSupplierStmt = setSupplierStatement(connection, item, supplierKey);
                updateSupplierStmt.executeUpdate();
            }

            // Bestellung mit finalem Gesamtpreis aktualisieren
            PreparedStatement updateOrderStmt = updateOrderStatement(connection, order, orderTotalPrice);
            updateOrderStmt.executeUpdate();

            // Transaktion abschließen
            connection.commit();

        } catch (SQLException e) {
            // Bei Fehler Rollback versuchen
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException rollbackEx) {
                throw new RuntimeException("Transaktion und Rollback fehlgeschlagen: " +
                        e.getMessage() + " & " + rollbackEx.getMessage());
            }
            throw new RuntimeException("Transaktion fehlgeschlagen: " + e.getMessage());
        }
    }


}
