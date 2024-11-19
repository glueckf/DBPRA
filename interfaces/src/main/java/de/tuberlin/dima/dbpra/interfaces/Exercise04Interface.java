package de.tuberlin.dima.dbpra.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

public interface Exercise04Interface {
    /**
     * See the slides for instructions.
     */
    void ex01CreateUDFs(Connection con) throws SQLException;

    /**
     * See the slides for instructions.
     */
    void ex02CreateView1(Connection con) throws SQLException;

    /**
     * See the slides for instructions.
     */
    void ex03CreateView2(Connection con) throws SQLException;

    /**
     * See the slides for instructions.
     */
    void ex04CreateTrigger(Connection con) throws SQLException;

    /**
     * See the slides for instructions.
     */
    void ex05CreateProcedure(Connection con) throws SQLException;
}
