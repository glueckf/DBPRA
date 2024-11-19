package de.tuberlin.dima.dbpra.interfaces.transactions;

public class PartSuppEntry {

    private int partkey;
    private int suppkey;
    private int avalqty;
    private double supplycost;

    public PartSuppEntry(int suppkey, int partkey, int avalqty, double supplycost) {
        this.suppkey = suppkey;
        this.partkey = partkey;
        this.supplycost = supplycost;
        this.avalqty = avalqty;
    }

    public int getSuppkey() {
        return suppkey;
    }

    public int getPartkey() {
        return partkey;
    }

    public int getAvalqty() {
        return avalqty;
    }

    public double getSupplycost() {
        return supplycost;
    }
}
