package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.interfaces.transactions.Order;
import de.tuberlin.dima.dbpra.interfaces.transactions.Lineitem;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 */
public class Exercise05Builder {

    private final static String ARTICLE_3_NAME = "brown blue puff midnight black";
    private final static String ARTICLE_5_NAME = "midnight linen almond tomato plum";
    private final static Date DATE = new Date();
    private final static Lineitem p3_valid = new Lineitem(3, 1, 10, 0.0, "N", "F", DATE, DATE, DATE, "NONE", "AIR");
    private final static Lineitem p3_invalid = new Lineitem(3, 1, 1000000, 0.0, "N", "F", DATE, DATE, DATE, "NONE", "AIR");
    private final static Lineitem p3_discount = new Lineitem(3, 1, 150, 0.0, "N", "F", DATE, DATE, DATE, "NONE", "AIR");
    private final static Lineitem p5_valid = new Lineitem(5, 2, 10, 0.0, "N", "F", DATE, DATE, DATE, "NONE", "AIR");
    private final static Lineitem p5_invalid = new Lineitem(5, 2, 1000000, 0.0, "N", "F", DATE, DATE, DATE, "NONE", "AIR");
    private final static Lineitem p5_discount = new Lineitem(5, 2, 150, 0.0, "N", "F", DATE, DATE, DATE, "NONE", "AIR");
    public static int initialOrderNr = 600001;

    public static Order createValidOrder() {
        List<Lineitem> lineitems = new ArrayList<Lineitem>();
        lineitems.add(p3_valid);
        lineitems.add(p5_valid);
        Order bst = new Order(initialOrderNr, lineitems, true);

        return bst;
    }

    public static Order createValidOrderSingle() {
        List<Lineitem> lineitems = new ArrayList<Lineitem>();
        lineitems.add(p3_valid);
        Order bst = new Order(initialOrderNr, lineitems, true);

        return bst;
    }

    public static Order createInValidOrder() {
        List<Lineitem> lineitems = new ArrayList<Lineitem>();
        lineitems.add(p3_invalid);
        lineitems.add(p5_invalid);
        Order bst = new Order(initialOrderNr, lineitems, true);

        return bst;
    }

    public static Order createNullOrder() {
        List<Lineitem> lineitems = new ArrayList<Lineitem>();
        lineitems.add(p3_valid);
        lineitems.add(null);
        lineitems.add(p5_valid);
        Order bst = new Order(initialOrderNr, lineitems, true);

        return bst;
    }

    public static Order createOrderWithDiscount() {
        List<Lineitem> lineitems = new ArrayList<Lineitem>();
        lineitems.add(p3_discount);
        lineitems.add(p5_discount);
        Order bst = new Order(initialOrderNr, lineitems, true);

        return bst;
    }

    public static Order createOrderWithoutDiscount() {
        List<Lineitem> lineitems = new ArrayList<Lineitem>();
        lineitems.add(p3_valid);
        lineitems.add(p5_valid);
        Order bst = new Order(initialOrderNr, lineitems, true);

        return bst;
    }
}
