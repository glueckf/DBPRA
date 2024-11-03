package de.tuberlin.dima.dbpra.exercises;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Exercise02 {

    /**
     * @return SQL Query für Aufgabe 1
     */
    static public String getSQLQuery01() {
        return getQueryString(1);
    }

    /**
     * @return SQL Query für Aufgabe 2
     */
    static public String getSQLQuery02() {
        return getQueryString(2);
    }

    /**
     * @return SQL Query für Aufgabe 3
     */
    static public String getSQLQuery03() {
        return getQueryString(3);
    }

    /**
     * @return SQL Query für Aufgabe 4
     */
    static public String getSQLQuery04() {
        return getQueryString(4);
    }

    /**
     * @return SQL Query für Aufgabe 5
     */
    static public String getSQLQuery05() {
        return getQueryString(5);
    }

    /**
     * @return SQL Query für Aufgabe 6
     */
    static public String getSQLQuery06() {
        return getQueryString(6);
    }

    /**
     * @return SQL Query für Aufgabe 7
     */
    static public String getSQLQuery07() {
        return getQueryString(7);
    }

    /**
     * @return SQL Query für Aufgabe 8
     */
    static public String getSQLQuery08() {
        return getQueryString(8);
    }

    /**
     * @return SQL Query für Aufgabe 9
     */
    static public String getSQLQuery09() {
        return getQueryString(9);
    }

    /**
     * @return SQL Query für Aufgabe 10
     */
    static public String getSQLQuery10() {
        return getQueryString(10);
    }

    static private String getQueryString(int i) {
        StringBuilder query = new StringBuilder();

        try {
            String path = String.format("SQL2Query%02d.sql", i);
            InputStream is = Exercise02.class.getClassLoader().getResourceAsStream(path);
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
