package de.tuberlin.dima.dbpra.interfaces.transactions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Lineitem {

    private static DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    private int partkey;
    private int linenumber;
    private int quantity;
    private double discount;
    private String returnflag;
    private String linestatus;
    private Date shipdate;
    private Date commitdate;
    private Date receiptdate;
    private String shipinstruct;
    private String shipmode;

    public Lineitem(int partkey, int linenumber, int quantity,
                         double discount, String returnflag, String linestatus,
                         Date shipdate, Date commitdate, Date receiptdate,
                         String shipinstruct, String shipmode) {
        this.partkey = partkey;
        this.linenumber = linenumber;
        this.quantity = quantity;
        this.discount = discount;
        this.returnflag = returnflag;
        this.linestatus = linestatus;
        this.shipdate = shipdate;
        this.commitdate = commitdate;
        this.receiptdate = receiptdate;
        this.shipinstruct = shipinstruct;
        this.shipmode = shipmode;
    }

    public int getPartkey() {
        return partkey;
    }

    public int getLinenumber() {
        return linenumber;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getDiscount() {
        return discount;
    }

    public String getReturnflag() {
        return returnflag;
    }

    public String getLinestatus() {
        return linestatus;
    }

    public String getShipdate() {
        return format.format(shipdate);
    }

    public String getCommitdate() {
        return format.format(commitdate);
    }

    public String getReceiptdate() {
        return format.format(receiptdate);
    }

    public String getShipinstruct() {
        return shipinstruct;
    }

    public String getShipmodet() {
        return shipmode;
    }
}
