package de.tuberlin.dima.dbpra.interfaces.transactions;

import java.util.Iterator;
import java.util.List;

public class Supplier{

    public List<PartSuppEntry> partSuppEntries;
    private int suppkey;
    private String name;
    private String address;
    private int nationkey;
    private String phone;
    private double acctbal;
    private int type;

    public Supplier(int suppkey, List<PartSuppEntry> partSuppEntries, int type) {
        this(suppkey, null, null, -1, null, partSuppEntries, type);
    }

    public Supplier(int suppkey, String name, String address, int nationkey, String phone, List<PartSuppEntry> partSuppEntries, int type) {
        this.suppkey = suppkey;
        this.name = name;
        this.address = address;
        this.nationkey = nationkey;
        this.phone = phone;
        this.acctbal = 0.0d;
        this.partSuppEntries= partSuppEntries;
        this.type = type;
    }

    public Iterator<PartSuppEntry> getPartSuppEntries() {
        return partSuppEntries.iterator();
    }

    public int getSuppkey() {
        return suppkey;
    }

    public String getName() {
        return name;
    }

    public String getAddress() {
        return address;
    }

    public int getNationkey() {
        return nationkey;
    }

    public String getPhone() {
        return phone;
    }

    public double getAcctbal() {
        return acctbal;
    }
}
