package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.interfaces.Exercise04Interface;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Exercise04 implements Exercise04Interface {

    /**
     * Task 1 (see slides)
     */
    public void ex01CreateUDFs(Connection con) throws SQLException {
        // your code comes here
        executeStatement(con, getQueryString(1));
        executeStatement(con, getQueryString(2));
    }

    /**
     * Task 2 (see slides)
     */
    public void ex02CreateView1(Connection con) throws SQLException {
        // your code comes here
    }

    /**
     * Task 3 (see slides)
     */
    public void ex03CreateView2(Connection con) throws SQLException {
        // your code comes here
    }

    /**
     * Task 4 (see slides)
     */
    public void ex04CreateTrigger(Connection con) throws SQLException {
        // your code comes here
    }

    /**
     * Task 5 (see slides)
     */
    public void ex05CreateProcedure(Connection con) throws SQLException {
        // your code comes here
    }

    private void executeStatement(Connection con, String query) throws SQLException {
        Statement statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.execute(query);
    }

    private String getQueryString(int i) {
        StringBuilder query = new StringBuilder();

        try {
            String path = String.format("SQL4Query%02d.sql", i);
            InputStream is = Exercise04.class.getClassLoader().getResourceAsStream(path);
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
