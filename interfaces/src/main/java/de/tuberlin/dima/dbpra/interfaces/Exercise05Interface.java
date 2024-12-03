package de.tuberlin.dima.dbpra.interfaces;

import de.tuberlin.dima.dbpra.interfaces.transactions.Order;
import de.tuberlin.dima.dbpra.interfaces.transactions.Supplier;

import java.sql.Connection;

public interface Exercise05Interface {
    void editOrder(Connection connection, Order order);


    void insertPartSuppEntry(Connection connection, Supplier supplier);
}
