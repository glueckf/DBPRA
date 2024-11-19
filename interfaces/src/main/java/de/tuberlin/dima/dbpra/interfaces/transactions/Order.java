package de.tuberlin.dima.dbpra.interfaces.transactions;

import java.util.Iterator;
import java.util.List;

public class Order {

    private int orderkey;
    private List<Lineitem> lineitems;

    public Order(int orderkey, List<Lineitem> lineitems, boolean valid) {
        this.orderkey = orderkey;
        this.lineitems = lineitems;
    }

    public Iterator<Lineitem> getLineitems() {
        return lineitems.iterator();
    }

    public int getOrderkey() {
        return orderkey;
    }

    public int getCustkey() {
        return 4450;
    }

    public String getOrderstatus() {
        return "O";
    }

    public String getOrderpriority() {
        return "4-NOT SPECIFIED";
    }

    public String getClerk() {
        return "Clerk#000000042";
    }

    public int getShippriority() {
        return 0;
    }
}
