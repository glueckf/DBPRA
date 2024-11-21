package de.tuberlin.dima.dbpra.exercises;

import de.tuberlin.dima.dbpra.config.ConnectionConfig;
import de.tuberlin.dima.dbpra.interfaces.Exercise04Interface;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Exercise04Test {

    private static Connection con;
    private static double[] points = {1.5, 1.5, 1.5, 2.5, 3};    // points for each exercise
    private static double[] scale = {0, 0, 0, 0, 0};         // scaling factors for each exercise (in [0;1])
    private static DecimalFormat format = new DecimalFormat("0.00");
    private static int testCtr = 0;     // test counter
    private static Exercise04Interface studentInstance;
    private static boolean debug = false;

    @AfterClass
    public static void summarize() {
        System.out.println("Group achieved " + format.format(sum(points, scale)) + " points.");
        System.out.println("");
    }

    // sum up array of doubles with scaling factors
    private static double sum(double[] points_in, double[] scale) {
        double s = 0;
        for (int i = 0; i < points_in.length; ++i)
            s += points[i] * scale[i];
        return s;
    }

    private static void printTestStart(String s) {
        System.out.println("Exercise " + ((int) testCtr + 1) + ": test for " + s + " starts ...");
    }

    public static void cleanTables(Connection con) {
        try {
            con.createStatement().executeUpdate("DROP TABLE PLACES_IN_UK");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: PLACES_IN_UK did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP TABLE BARS_IN_UK");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: BARS_IN_UK did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP TABLE BARS_WITH_AFFILIATIONS");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: BARS_WITH_AFFILIATIONS did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP TABLE BARS_WITH_AFFILIATIONS_SOLUTION");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: BARS_WITH_AFFILIATIONS_SOLUTION did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP VIEW DISTANCE_PLACES");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: DISTANCE_PLACES did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP TABLE DISTANCE_PLACES_SOLUTION");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: DISTANCE_PLACES_SOLUTION did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP VIEW AVG_DISTANCE");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: AVG_DISTANCE did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP TABLE AVG_DISTANCE_SOLUTION");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: AVG_DISTANCE_SOLUTION did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP function HAVERSINE");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: Function HAVERSINE not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP function ROUND4");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: Functions ROUND4 not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP TRIGGER DISTANCE_PLACES_UPDATE");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: DISTANCE_PLACES_UPDATE did not exist");
            }
        }

        try {
            con.createStatement().executeUpdate("DROP PROCEDURE ComputeAffiliationsOfBars");
        } catch (SQLException e) {
            if (debug) {
                System.out.println("WARNING: ComputeAffiliationsOfBars tables did not exist");
            }
        }
    }

    public static void createAndPopulateBarsUk(Connection con) {
        try {
            con.createStatement().executeUpdate("create table BARS_IN_UK(ID INTEGER not null, NAME VARCHAR(50), LATITUDE DOUBLE, LONGITUDE DOUBLE, PRIMARY KEY(ID))");
        } catch (SQLException e) {
            fail("ERROR: creation of table 'BARS_IN_UK' failed!");
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO BARS_IN_UK(ID, NAME, LATITUDE, LONGITUDE) VALUES " +
                    "(0, 'New Inn', 51.581954, 0.210429)," +
                    "(1, 'Montrose Bowling Club', 56.715206, -2.471487)," +
                    "(2, 'Crown Point Inn', 51.277347, 0.256815)," +
                    "(3, 'Stanhill Bar and Restaurant', 53.744808, -2.41598)," +
                    "(4, 'Wagon And Horses', 50.824093, -0.138391)," +
                    "(5, 'Goldstone Club', 50.832791, -0.173208)," +
                    "(6, 'Markie Dans', 56.418331, -5.474776)," +
                    "(7, 'Three Steps', 51.52987, -0.48241)," +
                    "(8, 'Thorn Tree Inn', 53.049737, -1.407655)," +
                    "(9, 'The Rogue Saint', 53.229607, -0.541885)," +
                    "(10, 'Plough and Harrow', 51.671368, -3.958084)," +
                    "(11, 'The Royal Oak and Premier Inn', 53.332853, -2.976307)," +
                    "(12, 'Skerton Liberal Club', 54.054237, -2.7995)," +
                    "(13, 'Bridgend Deaf Sports and Social Club', 51.503386, -3.563608)," +
                    "(14, 'The Greyhound', 51.364891, -0.164584)," +
                    "(15, 'High Throston Golf Club', 54.695561, -1.249767)," +
                    "(16, 'Alberrys Wine Bar', 51.277653, 1.078563)," +
                    "(17, 'Middlewich Masonic Hall', 53.192874, -2.441746)," +
                    "(18, 'The Rose Inn', 52.620766, 1.300132)," +
                    "(19, 'The Napier Arms', 51.386364, 0.541691)," +
                    "(20, 'Fresher & Professor', 50.374041, -4.136283)," +
                    "(21, 'The Thrasher Public House', 52.030938, 1.201562)," +
                    "(22, 'Boogie Bar', 53.522753, -1.131522)," +
                    "(23, 'The Drawing Board', 52.289792, -1.531403)," +
                    "(24, 'Museum Tavern', 51.518196, -0.125838)," +
                    "(25, 'The Earl Of Portsmouth', 50.899255, -3.833934)," +
                    "(26, 'Oxhill Central WMC & Institute', 54.865807, -1.709835)," +
                    "(27, 'The Miners Arms', 51.053125, -3.798943)," +
                    "(28, 'Railway Inn', 52.326981, -3.439535)," +
                    "(29, 'Egham Town Football Club Ltd', 51.426363, -0.532014)," +
                    "(30, 'Ivy Bush', 52.472139, -1.930735)," +
                    "(31, 'The Bridge Inn', 52.721471, 1.108447)," +
                    "(32, 'The George and Dragon', 52.43908, 0.932239)," +
                    "(33, 'UNDERGROUND KLUB', 57.14492, -2.10203)," +
                    "(34, 'Twinstead Cricket Club', 52.050076, 0.746907)," +
                    "(35, 'Queen Adelaide', 51.506614, -0.237846)," +
                    "(36, 'White Horse Inn', 52.267943, -1.121043)," +
                    "(37, 'Cutter Hotel', 50.855774, 0.589274)," +
                    "(38, 'BR RAILWAY SPORTS SOCIAL CLUB', 50.785392, -0.677272)," +
                    "(39, 'Mount Batten Bar & Carvery', 50.358717, -4.126835)," +
                    "(40, 'ShuffleDog', 53.800934, -1.537918)," +
                    "(41, 'White Hart Inn', 53.832772, -2.336504)," +
                    "(42, 'Kitty O''Sheas', 51.721905, -3.855617)," +
                    "(43, 'Ilford Cricket Club', 51.563178, 0.067846)," +
                    "(44, 'Fountain Inn (Bar Only)', 53.820042, -1.721308)," +
                    "(45, 'The Plume of Feathers PH', 51.971285, -0.280137)," +
                    "(46, 'Rainworth Miners Welfare Club', 53.118957, -1.112903)," +
                    "(47, 'Mojitos', 53.612565, -2.209383)," +
                    "(48, 'The Stilton Cheese', 52.491389, -0.289479)," +
                    "(49, 'Unique Food & Bars Ltd', 51.886635, -0.522285)," +
                    "(50, 'The Otter', 50.998523, -1.354133)," +
                    "(51, 'Moorcock Inn', 53.693167, -1.919987)," +
                    "(52, 'Red Lion', 53.688359, -1.582276)," +
                    "(53, 'Crab And Lobster', 54.17215, -1.392739)," +
                    "(54, 'Rose & Crown', 53.924105, -1.183335)," +
                    "(55, 'Cross Foxes Inn', 52.977941, -2.963758)," +
                    "(56, 'The Black Horse', 54.943509, -1.454951)," +
                    "(57, 'The Crown Inn', 51.011589, -2.279901)," +
                    "(58, 'Denholme Conservative Club', 53.803943, -1.894618)," +
                    "(59, 'The Earl Of Zetland', 56.019205, -3.722087)," +
                    "(60, 'Kings Arms Inn', 52.088984, -1.76903)," +
                    "(61, 'Grant Arms', 52.417575, -1.925733)," +
                    "(62, 'The Lockwood Arms', 53.630909, -1.794156)," +
                    "(63, 'Dying Gladiator', 53.551576, -0.490824)," +
                    "(64, 'The Jenny Wren', 51.350242, 0.71688)," +
                    "(65, 'Trimley Sports And Social Club', 51.989622, 1.308764)," +
                    "(66, 'Old Albion Inn', 50.402426, -5.109467)," +
                    "(67, 'The Cuckfield Public House', 51.579701, 0.0243)," +
                    "(68, 'Miners Arms', 53.474879, -1.46541)," +
                    "(69, 'Cross Keys Inn', 54.233652, -1.344815)," +
                    "(70, 'Ballers', 53.523076, -1.131545)," +
                    "(71, 'The Langley Park Hotel', 54.800313, -1.670685)," +
                    "(72, 'Station Hotel', 53.48938, -0.99092)," +
                    "(73, 'Cannards Well Hotel', 51.175296, -2.535718)," +
                    "(74, 'T.S. Black Swan', 51.39058, -0.423868)," +
                    "(75, 'Black Market V.I.P', 50.856107, 0.589662)," +
                    "(76, 'St William''s Social Club', 54.606674, -1.093752)," +
                    "(77, 'White Swan', 51.752222, -0.340612)," +
                    "(78, 'Alexanders Bar', 55.900503, -4.405173)," +
                    "(79, 'Northam Social Club', 50.910533, -1.389289)," +
                    "(80, 'The Hub and Terrace Bar', 54.572085, -1.234711)," +
                    "(81, 'The Dormouse', 53.97682, -1.109942)," +
                    "(82, 'Old Chain Yard''s Ltd', 52.541708, -2.087701)," +
                    "(83, 'THE 1224 CLUB [MASONIC LODGE]', 57.096366, -2.267528)," +
                    "(84, 'Farnsfield Cricket Club', 53.105422, -1.036882)," +
                    "(85, 'The West Bulls', 53.770658, -0.391905)," +
                    "(86, 'The Old Mill', 52.543611, -2.001209)," +
                    "(87, 'Horton House Sports Club', 52.183627, -0.801183)," +
                    "(88, 'Great & Little Warley Cricket Club', 51.592376, 0.282345)," +
                    "(89, 'Bay Horse Inn', 53.800193, -1.040943)," +
                    "(90, 'Fox & Hounds', 50.882237, -1.318413)," +
                    "(91, 'LLANELLI AFC', 51.683391, -4.147838)," +
                    "(92, 'Pestle N Mortar', 52.541728, -1.369821)," +
                    "(93, 'Guisborough Rugby Club', 54.531101, -1.048016)," +
                    "(94, 'Norley Hall Cricket Club', 53.544022, -2.681737)," +
                    "(95, 'Hillview Inn', 55.916976, -4.376537)," +
                    "(96, 'Main Street Bar & Grill', 55.86912, -4.550933)," +
                    "(97, 'Marquis Of Westminster Public House', 51.491901, -0.140516)," +
                    "(98, 'Oddfellows Arms (Northallerton) Limited', 54.341275, -1.437302)," +
                    "(99, 'Goudhurst Inn', 51.113627, 0.47171)"
            );
        } catch (SQLException e) {
            System.err.println(e.getLocalizedMessage());
            System.out.println("WARNING: could not import data into table 'BARS_IN_UK'");
        }
    }

    public static void createAndPopulatePlacesUk(Connection con) {
        try {
            con.createStatement().executeUpdate("create table PLACES_IN_UK(ID INTEGER not null, NAME VARCHAR(50), LATITUDE DOUBLE, LONGITUDE DOUBLE, PRIMARY KEY(ID))");
        } catch (SQLException e) {
            fail("ERROR: creation of table 'PLACES_IN_UK' failed!");
        }
        try {
            con.createStatement().executeUpdate("INSERT INTO PLACES_IN_UK(ID, NAME, LATITUDE, LONGITUDE) VALUES " +
                    "(0, 'York, North Yorkshire', 53.95833199999999, -1.080278)," +
                    "(1, 'Worcester, Worcestershire', 52.192001, -2.22)," +
                    "(2, 'Winchester, Hampshire', 51.063202000000004, -1.308)," +
                    "(3, 'Wells, South West England', 51.208999999999996, -2.647)," +
                    "(4, 'Wakefield, West Yorkshire', 53.68, -1.49)," +
                    "(5, 'Truro, Cornwall', 50.259997999999996, -5.051)," +
                    "(6, 'Sunderland, North East', 54.906101, -1.38113)," +
                    "(7, 'Sheffield, South Yorkshire', 53.383331000000005, -1.466667)," +
                    "(8, 'Salford, North West', 53.483002, -2.2931)," +
                    "(9, 'St. Davids, Wales', 51.882, -5.269)," +
                    "(10, 'St.Albans, Hertfordshire', 51.755001, -0.336)," +
                    "(11, 'Ripon, North Yorkshire', 54.138000000000005, -1.524)," +
                    "(12, 'Portsmouth, Hampshire', 50.805832, -1.087222)," +
                    "(13, 'Perth, Scotland', 56.396999, -3.437)," +
                    "(14, 'Nottingham', 52.950001, -1.15)," +
                    "(15, 'Newry, Northern Ireland', 54.175999, -6.349)," +
                    "(16, 'Newcastle Upon Tyne', 54.966667, -1.6)," +
                    "(17, 'Liverpool, Merseyside', 53.400002, -2.983333)," +
                    "(18, 'Lincoln, Lincolnshire', 53.234443999999996, -0.538611)," +
                    "(19, 'Lichfield, Staffordshire', 52.683498, -1.82653)," +
                    "(20, 'Leicester, the East Midlands', 52.633331000000005, -1.133333)," +
                    "(21, 'Lancaster, Lancashire', 54.047001, -2.801)," +
                    "(22, 'Hereford, Herefordshire', 52.056499, -2.716)," +
                    "(23, 'Gloucester, Gloucestershire', 51.864445, -2.2444439999999997)," +
                    "(24, 'Glasgow', 55.860916, -4.2514330000000005)," +
                    "(25, 'Exeter', 50.716667, -3.5333330000000003)," +
                    "(26, 'Ely, Cambridgeshire', 52.398056, 0.26222199999999996)," +
                    "(27, 'Durham', 54.7761, -1.5733)," +
                    "(28, 'Dundee, Scotland', 56.462002000000005, -2.9707)," +
                    "(29, 'Derry, Northern Ireland', 54.9958, -7.3074)," +
                    "(30, 'Derby, Derbyshire', 52.91666800000001, -1.466667)," +
                    "(31, 'Coventry, West Midlands', 52.408054, -1.510556)," +
                    "(32, 'Chichester, West Sussex', 50.836498, -0.7792)," +
                    "(33, 'Chester, Chesire', 53.189999, -2.89)," +
                    "(34, 'Chelmsford, Essex', 51.736098999999996, 0.4798)," +
                    "(35, 'Carlisle, North West', 54.890999, -2.944)," +
                    "(36, 'Canterbury, Kent', 51.279999, 1.08)," +
                    "(37, 'Cambridge, Cambridgeshire', 52.205276, 0.11916700000000001)," +
                    "(38, 'Brighton & Hove, East Sussex', 50.827778, -0.152778)," +
                    "(39, 'Bradford, West Yorkshire', 53.799999, -1.75)," +
                    "(40, 'Bath, Somerset', 51.380001, -2.36)," +
                    "(41, 'Peterborough, Cambridgeshire', 52.573921, -0.25083)," +
                    "(42, 'Elgin, Scotland', 57.653484, -3.335724)," +
                    "(43, 'Stoke-on-Trent, Staffordshire', 53.002666000000005, -2.179404)," +
                    "(44, 'Solihull, Birmingham', 52.412811, -1.778197)," +
                    "(45, 'Cardiff, Wales', 51.481583, -3.17909)," +
                    "(46, 'Eastbourne, East Sussex', 50.768035999999995, 0.29047199999999995)," +
                    "(47, 'Oxford', 51.752022, -1.257677)," +
                    "(48, 'London', 51.509865000000005, -0.118092)," +
                    "(49, 'Swindon, Swindon', 51.568535, -1.772232)," +
                    "(50, 'Gravesend, Kent', 51.441883000000004, 0.370759)," +
                    "(51, 'Northampton, Northamptonshire', 52.240479, -0.9026559999999999)," +
                    "(52, 'Rugby, Warwickshire', 52.370876, -1.265032)," +
                    "(53, 'Sutton Coldfield, West Midlands', 52.570384999999995, -1.8240419999999997)," +
                    "(54, 'Harlow, Essex', 51.772938, 0.10231)," +
                    "(55, 'Aberdeen', 57.149651, -2.099075)," +
                    "(56, 'Swansea', 51.621441, -3.9436459999999998)," +
                    "(57, 'Chesterfield, Derbyshire', 53.235046, -1.421629)," +
                    "(58, 'Lisburn, Ireland', 54.509719999999994, -6.0374)," +
                    "(59, 'Londonderry, Derry', 55.006763, -7.318267999999999)," +
                    "(60, 'Salisbury, Wiltshire', 51.068787, -1.794472)," +
                    "(61, 'Manchester', 53.483959, -2.244644)," +
                    "(62, 'Bristol', 51.454514, -2.58791)," +
                    "(63, 'Wolverhampton, West Midlands', 52.59137, -2.110748)," +
                    "(64, 'Preston, Lancashire', 53.765762, -2.6923369999999998)," +
                    "(65, 'Ayr, South Ayrshire', 55.458565, -4.629179)," +
                    "(66, 'Hastings, East Sussex', 50.854259000000006, 0.573453)," +
                    "(67, 'Bedford', 52.136435999999996, -0.460739)," +
                    "(68, 'Basildon, Essex', 51.572376, 0.470009)," +
                    "(69, 'Chippenham, Wiltshire', 51.458057000000004, -2.1160740000000002)," +
                    "(70, 'Birmingham, West Midlands', 52.489470999999995, -1.898575)," +
                    "(71, 'Folkestone, Kent', 51.081398, 1.169456)," +
                    "(72, 'Edinburgh, Scotland', 55.953251, -3.1882669999999997)," +
                    "(73, 'Southampton', 50.909698, -1.4043510000000001)," +
                    "(74, 'Belfast, Northern Ireland', 54.607868, -5.926437)," +
                    "(75, 'Uckfield, East Sussex', 50.967940999999996, 0.08583099999999999)," +
                    "(76, 'Worthing, West Sussex', 50.825024, -0.383835)," +
                    "(77, 'Leeds, West Yorkshire', 53.801277, -1.548567)," +
                    "(78, 'Kendal, Cumbria', 54.328506000000004, -2.7438700000000003)," +
                    "(79, 'Hull', 53.76775, -0.335827)," +
                    "(80, 'Plymouth', 50.376289, -4.143841)," +
                    "(81, 'Haverhill, Suffolk', 52.080875, 0.444517)," +
                    "(82, 'Frankton, Warwickshire', 52.328415, -1.377561)," +
                    "(83, 'Inverness', 57.477771999999995, -4.224721)"
            );

        } catch (SQLException e) {
            System.err.println(e.getLocalizedMessage());
            System.out.println("WARNING: could not import data into table 'PLACES_IN_UK'");
        }
    }

    public static void createAndPopulateDistancePlacesSolution(Connection con) {
        try {
            con.createStatement().executeUpdate("create table DISTANCE_PLACES_SOLUTION(ID INTEGER not null, NAME VARCHAR(50), LONGITUDE DOUBLE, LATITUDE DOUBLE, DISTANCE_LONDON DOUBLE, PRIMARY KEY(ID))");
        } catch (SQLException e) {
            fail("ERROR: creation of table 'DISTANCE_PLACES_SOLUTION' failed!");
        }
        try {
            con.createStatement().executeUpdate("insert into DISTANCE_PLACES_SOLUTION (ID, NAME, LONGITUDE, LATITUDE, DISTANCE_LONDON) values\n" +
                    "(0, 'York, North Yorkshire', -1.080278, 53.95833199999999, 279.85158),\n" +
                    "(1, 'Worcester, Worcestershire', -2.22, 52.192001, 163.0755),\n" +
                    "(2, 'Winchester, Hampshire', -1.308, 51.063202, 96.51007),\n" +
                    "(3, 'Wells, South West England', -2.647, 51.209, 178.74049),\n" +
                    "(4, 'Wakefield, West Yorkshire', -1.49, 53.68, 258.47607),\n" +
                    "(5, 'Truro, Cornwall', -5.051, 50.259998, 372.8171),\n" +
                    "(6, 'Sunderland, North East', -1.38113, 54.906101, 386.88089),\n" +
                    "(7, 'Sheffield, South Yorkshire', -1.466667, 53.383331000000005, 227.4773),\n" +
                    "(8, 'Salford, North West', -2.2931, 53.483002, 264.20212),\n" +
                    "(9, 'St. Davids, Wales', -5.269, 51.882, 357.34002),\n" +
                    "(10, 'St.Albans, Hertfordshire', -0.336, 51.755001, 31.13175),\n" +
                    "(11, 'Ripon, North Yorkshire', -1.524, 54.138000000000005, 307.10756),\n" +
                    "(12, 'Portsmouth, Hampshire', -1.087222, 50.805832, 103.42152),\n" +
                    "(13, 'Perth, Scotland', -3.437, 56.396999, 585.03879),\n" +
                    "(14, 'Nottingham', -1.15, 52.950001, 174.87432),\n" +
                    "(15, 'Newry, Northern Ireland', -6.349, 54.175999, 512.55064),\n" +
                    "(16, 'Newcastle Upon Tyne', -1.6, 54.966667, 396.80438),\n" +
                    "(17, 'Liverpool, Merseyside', -2.983333, 53.400002, 286.07968),\n" +
                    "(18, 'Lincoln, Lincolnshire', -0.538611, 53.234444, 193.87682),\n" +
                    "(19, 'Lichfield, Staffordshire', -1.82653, 52.683498, 175.0634),\n" +
                    "(20, 'Leicester, the East Midlands', -1.133333, 52.633331000000005, 142.89855),\n" +
                    "(21, 'Lancaster, Lancashire', -2.801, 54.047001, 334.83988),\n" +
                    "(22, 'Hereford, Herefordshire', -2.716, 52.056499, 188.75017),\n" +
                    "(23, 'Gloucester, Gloucestershire', -2.244444, 51.864445, 151.78556),\n" +
                    "(24, 'Glasgow', -4.2514330000000005, 55.860916, 554.89509),\n" +
                    "(25, 'Exeter', -3.5333330000000003, 50.716667, 254.16713),\n" +
                    "(26, 'Ely, Cambridgeshire', 0.262222, 52.398056, 102.14289),\n" +
                    "(27, 'Durham', -1.5733, 54.7761, 375.91199),\n" +
                    "(28, 'Dundee, Scotland', -2.9707, 56.462002000000005, 581.25146),\n" +
                    "(29, 'Derry, Northern Ireland', -7.3074, 54.9958, 615.10535),\n" +
                    "(30, 'Derby, Derbyshire', -1.466667, 52.91666800000001, 181.40962),\n" +
                    "(31, 'Coventry, West Midlands', -1.510556, 52.408054, 138.12003),\n" +
                    "(32, 'Chichester, West Sussex', -0.7792, 50.836498, 87.9225),\n" +
                    "(33, 'Chester, Chesire', -2.89, 53.189999, 265.19776),\n" +
                    "(34, 'Chelmsford, Essex', 0.4798, 51.736099, 48.3363),\n" +
                    "(35, 'Carlisle, North West', -2.944, 54.890999, 420.36785),\n" +
                    "(36, 'Canterbury, Kent', 1.08, 51.279999, 86.96336),\n" +
                    "(37, 'Cambridge, Cambridgeshire', 0.119167, 52.205276, 79.02413),\n" +
                    "(38, 'Brighton & Hove, East Sussex', -0.152778, 50.827778, 75.88316),\n" +
                    "(39, 'Bradford, West Yorkshire', -1.75, 53.799999, 277.40508),\n" +
                    "(40, 'Bath, Somerset', -2.36, 51.380001, 156.03672),\n" +
                    "(41, 'Peterborough, Cambridgeshire', -0.25083, 52.573921, 118.66536),\n" +
                    "(42, 'Elgin, Scotland', -3.335724, 57.653484, 713.70892),\n" +
                    "(43, 'Stoke-on-Trent, Staffordshire', -2.179404, 53.002666000000005, 217.32641),\n" +
                    "(44, 'Solihull, Birmingham', -1.778197, 52.412811, 151.71304),\n" +
                    "(45, 'Cardiff, Wales', -3.17909, 51.481583, 211.91149),\n" +
                    "(46, 'Eastbourne, East Sussex', 0.29047199999999995, 50.768036, 87.2734),\n" +
                    "(47, 'Oxford', -1.257677, 51.752022, 83.136),\n" +
                    "(49, 'Swindon, Swindon', -1.772232, 51.568535, 114.58524),\n" +
                    "(50, 'Gravesend, Kent', 0.370759, 51.441883, 34.68996),\n" +
                    "(51, 'Northampton, Northamptonshire', -0.902656, 52.240479, 97.4712),\n" +
                    "(52, 'Rugby, Warwickshire', -1.265032, 52.370876, 123.88222),\n" +
                    "(53, 'Sutton Coldfield, West Midlands', -1.824042, 52.570385, 165.88527),\n" +
                    "(54, 'Harlow, Essex', 0.10231, 51.772938, 32.96986),\n" +
                    "(55, 'Aberdeen', -2.099075, 57.149651, 640.06289),\n" +
                    "(56, 'Swansea', -3.943646, 51.621441, 264.68555),\n" +
                    "(57, 'Chesterfield, Derbyshire', -1.421629, 53.235046, 211.25015),\n" +
                    "(58, 'Lisburn, Ireland', -6.0374, 54.50972, 517.4824),\n" +
                    "(59, 'Londonderry, Derry', -7.318267999999999, 55.006763, 616.38385),\n" +
                    "(60, 'Salisbury, Wiltshire', -1.794472, 51.068787, 126.46871),\n" +
                    "(61, 'Manchester', -2.244644, 53.483959, 262.47759),\n" +
                    "(62, 'Bristol', -2.58791, 51.454514, 171.13133),\n" +
                    "(63, 'Wolverhampton, West Midlands', -2.110748, 52.59137, 181.72577),\n" +
                    "(64, 'Preston, Lancashire', -2.692337, 53.765762, 305.07344),\n" +
                    "(65, 'Ayr, South Ayrshire', -4.629179, 55.458565, 530.67842),\n" +
                    "(66, 'Hastings, East Sussex', 0.573453, 50.854259000000006, 87.39404),\n" +
                    "(67, 'Bedford', -0.460739, 52.136436, 73.54368),\n" +
                    "(68, 'Basildon, Essex', 0.470009, 51.572376, 41.26145),\n" +
                    "(69, 'Chippenham, Wiltshire', -2.116074, 51.458057, 138.46549),\n" +
                    "(70, 'Birmingham, West Midlands', -1.898575, 52.489471, 163.46077),\n" +
                    "(71, 'Folkestone, Kent', 1.169456, 51.081398, 101.40994),\n" +
                    "(72, 'Edinburgh, Scotland', -3.188267, 55.953251, 533.63087),\n" +
                    "(73, 'Southampton', -1.404351, 50.909698, 111.72019),\n" +
                    "(74, 'Belfast, Northern Ireland', -5.926437, 54.607868, 518.67149),\n" +
                    "(75, 'Uckfield, East Sussex', 0.08583099999999999, 50.967941, 61.9088),\n" +
                    "(76, 'Worthing, West Sussex', -0.383835, 50.825024, 78.37246),\n" +
                    "(77, 'Leeds, West Yorkshire', -1.548567, 53.801277, 272.43637),\n" +
                    "(78, 'Kendal, Cumbria', -2.7438700000000003, 54.328506, 359.41572),\n" +
                    "(79, 'Hull', -0.335827, 53.76775, 251.49454),\n" +
                    "(80, 'Plymouth', -4.143841, 50.376289, 308.88453),\n" +
                    "(81, 'Haverhill, Suffolk', 0.444517, 52.080875, 74.3528),\n" +
                    "(82, 'Frankton, Warwickshire', -1.377561, 52.328415, 125.4768),\n" +
                    "(83, 'Inverness', -4.224721, 57.477772, 714.31371)"
            );

        } catch (SQLException e) {
            System.err.println(e.getLocalizedMessage());
            System.out.println("WARNING: could not import data into table 'DISTANCE_PLACES_SOLUTION'");
        }
    }

    public static void createAndPopulateBarsWithAffiliationsSolution(Connection con) {
        try {
            con.createStatement().executeUpdate("create table BARS_WITH_AFFILIATIONS_SOLUTION(ID INTEGER not null, BARNAME VARCHAR(50), PLACENAME VARCHAR(50), PRIMARY KEY(ID))");
        } catch (SQLException e) {
            fail("ERROR: creation of table 'BARS_WITH_AFFILIATIONS_KORREKT' failed!");
        }
        try {
            con.createStatement().executeUpdate("insert into BARS_WITH_AFFILIATIONS_SOLUTION (ID, BARNAME, PLACENAME)\n" +
                    "values  (0, 'New Inn', 'Basildon, Essex'),\n" +
                    "(1, 'Montrose Bowling Club', 'Dundee, Scotland'),\n" +
                    "(2, 'Crown Point Inn', 'Gravesend, Kent'),\n" +
                    "(3, 'Stanhill Bar and Restaurant', 'Preston, Lancashire'),\n" +
                    "(4, 'Wagon And Horses', 'Brighton & Hove, East Sussex'),\n" +
                    "(5, 'Goldstone Club', 'Brighton & Hove, East Sussex'),\n" +
                    "(6, 'Markie Dans', 'Glasgow'),\n" +
                    "(7, 'Three Steps', 'London'),\n" +
                    "(8, 'Thorn Tree Inn', 'Derby, Derbyshire'),\n" +
                    "(9, 'The Rogue Saint', 'Lincoln, Lincolnshire'),\n" +
                    "(10, 'Plough and Harrow', 'Swansea'),\n" +
                    "(11, 'The Royal Oak and Premier Inn', 'Liverpool, Merseyside'),\n" +
                    "(12, 'Skerton Liberal Club', 'Lancaster, Lancashire'),\n" +
                    "(13, 'Bridgend Deaf Sports and Social Club', 'Cardiff, Wales'),\n" +
                    "(14, 'The Greyhound', 'London'),\n" +
                    "(15, 'High Throston Golf Club', 'Durham'),\n" +
                    "(16, 'Alberrys Wine Bar', 'Canterbury, Kent'),\n" +
                    "(17, 'Middlewich Masonic Hall', 'Stoke-on-Trent, Staffordshire'),\n" +
                    "(18, 'The Rose Inn', 'Ely, Cambridgeshire'),\n" +
                    "(19, 'The Napier Arms', 'Gravesend, Kent'),\n" +
                    "(20, 'Fresher & Professor', 'Plymouth'),\n" +
                    "(21, 'The Thrasher Public House', 'Haverhill, Suffolk'),\n" +
                    "(22, 'Boogie Bar', 'Sheffield, South Yorkshire'),\n" +
                    "(23, 'The Drawing Board', 'Frankton, Warwickshire'),\n" +
                    "(24, 'Museum Tavern', 'London'),\n" +
                    "(25, 'The Earl Of Portsmouth', 'Exeter'),\n" +
                    "(26, 'Oxhill Central WMC & Institute', 'Newcastle Upon Tyne'),\n" +
                    "(27, 'The Miners Arms', 'Exeter'),\n" +
                    "(28, 'Railway Inn', 'Hereford, Herefordshire'),\n" +
                    "(29, 'Egham Town Football Club Ltd', 'London'),\n" +
                    "(30, 'Ivy Bush', 'Birmingham, West Midlands'),\n" +
                    "(31, 'The Bridge Inn', 'Ely, Cambridgeshire'),\n" +
                    "(32, 'The George and Dragon', 'Ely, Cambridgeshire'),\n" +
                    "(33, 'UNDERGROUND KLUB', 'Aberdeen'),\n" +
                    "(34, 'Twinstead Cricket Club', 'Haverhill, Suffolk'),\n" +
                    "(35, 'Queen Adelaide', 'London'),\n" +
                    "(36, 'White Horse Inn', 'Rugby, Warwickshire'),\n" +
                    "(37, 'Cutter Hotel', 'Hastings, East Sussex'),\n" +
                    "(38, 'BR RAILWAY SPORTS SOCIAL CLUB', 'Chichester, West Sussex'),\n" +
                    "(39, 'Mount Batten Bar & Carvery', 'Plymouth'),\n" +
                    "(40, 'ShuffleDog', 'Leeds, West Yorkshire'),\n" +
                    "(41, 'White Hart Inn', 'Preston, Lancashire'),\n" +
                    "(42, 'Kitty O''Sheas', 'Swansea'),\n" +
                    "(43, 'Ilford Cricket Club', 'London'),\n" +
                    "(44, 'Fountain Inn (Bar Only)', 'Bradford, West Yorkshire'),\n" +
                    "(45, 'The Plume of Feathers PH', 'Bedford'),\n" +
                    "(46, 'Rainworth Miners Welfare Club', 'Nottingham'),\n" +
                    "(47, 'Mojitos', 'Manchester'),\n" +
                    "(48, 'The Stilton Cheese', 'Peterborough, Cambridgeshire'),\n" +
                    "(49, 'Unique Food & Bars Ltd', 'St.Albans, Hertfordshire'),\n" +
                    "(50, 'The Otter', 'Winchester, Hampshire'),\n" +
                    "(51, 'Moorcock Inn', 'Bradford, West Yorkshire'),\n" +
                    "(52, 'Red Lion', 'Wakefield, West Yorkshire'),\n" +
                    "(53, 'Crab And Lobster', 'Ripon, North Yorkshire'),\n" +
                    "(54, 'Rose & Crown', 'York, North Yorkshire'),\n" +
                    "(55, 'Cross Foxes Inn', 'Chester, Chesire'),\n" +
                    "(56, 'The Black Horse', 'Sunderland, North East'),\n" +
                    "(57, 'The Crown Inn', 'Wells, South West England'),\n" +
                    "(58, 'Denholme Conservative Club', 'Bradford, West Yorkshire'),\n" +
                    "(59, 'The Earl Of Zetland', 'Edinburgh, Scotland'),\n" +
                    "(60, 'Kings Arms Inn', 'Worcester, Worcestershire'),\n" +
                    "(61, 'Grant Arms', 'Birmingham, West Midlands'),\n" +
                    "(62, 'The Lockwood Arms', 'Bradford, West Yorkshire'),\n" +
                    "(63, 'Dying Gladiator', 'Hull'),\n" +
                    "(64, 'The Jenny Wren', 'Gravesend, Kent'),\n" +
                    "(65, 'Trimley Sports And Social Club', 'Haverhill, Suffolk'),\n" +
                    "(66, 'Old Albion Inn', 'Truro, Cornwall'),\n" +
                    "(67, 'The Cuckfield Public House', 'London'),\n" +
                    "(68, 'Miners Arms', 'Sheffield, South Yorkshire'),\n" +
                    "(69, 'Cross Keys Inn', 'Ripon, North Yorkshire'),\n" +
                    "(70, 'Ballers', 'Sheffield, South Yorkshire'),\n" +
                    "(71, 'The Langley Park Hotel', 'Durham'),\n" +
                    "(72, 'Station Hotel', 'Sheffield, South Yorkshire'),\n" +
                    "(73, 'Cannards Well Hotel', 'Wells, South West England'),\n" +
                    "(74, 'T.S. Black Swan', 'London'),\n" +
                    "(75, 'Black Market V.I.P', 'Hastings, East Sussex'),\n" +
                    "(76, 'St William''s Social Club', 'Durham'),\n" +
                    "(77, 'White Swan', 'St.Albans, Hertfordshire'),\n" +
                    "(78, 'Alexanders Bar', 'Glasgow'),\n" +
                    "(79, 'Northam Social Club', 'Southampton'),\n" +
                    "(80, 'The Hub and Terrace Bar', 'Durham'),\n" +
                    "(81, 'The Dormouse', 'York, North Yorkshire'),\n" +
                    "(82, 'Old Chain Yard''s Ltd', 'Wolverhampton, West Midlands'),\n" +
                    "(83, 'THE 1224 CLUB [MASONIC LODGE]', 'Aberdeen'),\n" +
                    "(84, 'Farnsfield Cricket Club', 'Nottingham'),\n" +
                    "(85, 'The West Bulls', 'Hull'),\n" +
                    "(86, 'The Old Mill', 'Wolverhampton, West Midlands'),\n" +
                    "(87, 'Horton House Sports Club', 'Northampton, Northamptonshire'),\n" +
                    "(88, 'Great & Little Warley Cricket Club', 'Basildon, Essex'),\n" +
                    "(89, 'Bay Horse Inn', 'York, North Yorkshire'),\n" +
                    "(90, 'Fox & Hounds', 'Southampton'),\n" +
                    "(91, 'LLANELLI AFC', 'Swansea'),\n" +
                    "(92, 'Pestle N Mortar', 'Coventry, West Midlands'),\n" +
                    "(93, 'Guisborough Rugby Club', 'Durham'),\n" +
                    "(94, 'Norley Hall Cricket Club', 'Preston, Lancashire'),\n" +
                    "(95, 'Hillview Inn', 'Glasgow'),\n" +
                    "(96, 'Main Street Bar & Grill', 'Glasgow'),\n" +
                    "(97, 'Marquis Of Westminster Public House', 'London'),\n" +
                    "(98, 'Oddfellows Arms (Northallerton) Limited', 'Ripon, North Yorkshire'),\n" +
                    "(99, 'Goudhurst Inn', 'Hastings, East Sussex')"
            );

        } catch (SQLException e) {
            System.err.println(e.getLocalizedMessage());
            System.out.println("WARNING: could not import data into table 'BARS_WITH_AFFILIATIONS_SOLUTION'");
        }
    }

    public static void createAndPopulateAvgDistanceSolution(Connection con) {
        try {
            con.createStatement().executeUpdate("create table AVG_DISTANCE_SOLUTION(ID INTEGER not null, NAME VARCHAR(50), AVG_DISTANCE DOUBLE, PRIMARY KEY(ID))");
        } catch (SQLException e) {
            fail("ERROR: creation of table 'AVG_DISTANCE' failed!");
        }
        try {
            con.createStatement().executeUpdate("insert into AVG_DISTANCE_SOLUTION (ID, NAME, AVG_DISTANCE) " +
                    "values " +
                    "(0, 'York, North Yorkshire', 229.0827),\n" +
                    "(1, 'Worcester, Worcestershire', 171.6842),\n" +
                    "(2, 'Winchester, Hampshire', 214.2432),\n" +
                    "(3, 'Wells, South West England', 218.9927),\n" +
                    "(4, 'Wakefield, West Yorkshire', 205.464),\n" +
                    "(5, 'Truro, Cornwall', 373.4388),\n" +
                    "(6, 'Sunderland, North East', 306.0261),\n" +
                    "(7, 'Sheffield, South Yorkshire', 189.2263),\n" +
                    "(8, 'Salford, North West', 197.7914),\n" +
                    "(9, 'St. Davids, Wales', 302.7356),\n" +
                    "(10, 'St.Albans, Hertfordshire', 192.7381),\n" +
                    "(11, 'Ripon, North Yorkshire', 238.0062),\n" +
                    "(12, 'Portsmouth, Hampshire', 234.856),\n" +
                    "(14, 'Nottingham', 174.903),\n" +
                    "(15, 'Newry, Northern Ireland', 385.929),\n" +
                    "(16, 'Newcastle Upon Tyne', 310.5517),\n" +
                    "(17, 'Liverpool, Merseyside', 209.684),\n" +
                    "(18, 'Lincoln, Lincolnshire', 198.9109),\n" +
                    "(19, 'Lichfield, Staffordshire', 165.8522),\n" +
                    "(20, 'Leicester, the East Midlands', 167.8039),\n" +
                    "(21, 'Lancaster, Lancashire', 241.4739),\n" +
                    "(22, 'Hereford, Herefordshire', 186.0786),\n" +
                    "(23, 'Gloucester, Gloucestershire', 179.6693),\n" +
                    "(25, 'Exeter', 279.3921),\n" +
                    "(26, 'Ely, Cambridgeshire', 205.6948),\n" +
                    "(27, 'Durham', 292.6144),\n" +
                    "(29, 'Derry, Northern Ireland', 476.1367),\n" +
                    "(30, 'Derby, Derbyshire', 170.872),\n" +
                    "(31, 'Coventry, West Midlands', 163.0198),\n" +
                    "(32, 'Chichester, West Sussex', 235.5588),\n" +
                    "(33, 'Chester, Chesire', 199.175),\n" +
                    "(34, 'Chelmsford, Essex', 222.0888),\n" +
                    "(35, 'Carlisle, North West', 312.7773),\n" +
                    "(36, 'Canterbury, Kent', 268.7927),\n" +
                    "(37, 'Cambridge, Cambridgeshire', 199.5129),\n" +
                    "(38, 'Brighton & Hove, East Sussex', 248.5108),\n" +
                    "(39, 'Bradford, West Yorkshire', 212.4017),\n" +
                    "(40, 'Bath, Somerset', 202.3634),\n" +
                    "(41, 'Peterborough, Cambridgeshire', 188.2607),\n" +
                    "(43, 'Stoke-on-Trent, Staffordshire', 177.4247),\n" +
                    "(44, 'Solihull, Birmingham', 163.6261),\n" +
                    "(45, 'Cardiff, Wales', 220.1139),\n" +
                    "(46, 'Eastbourne, East Sussex', 266.6828),\n" +
                    "(47, 'Oxford', 177.9104),\n" +
                    "(48, 'London', 208.1734),\n" +
                    "(49, 'Swindon, Swindon', 185.4252),\n" +
                    "(50, 'Gravesend, Kent', 228.254),\n" +
                    "(51, 'Northampton, Northamptonshire', 170.3388),\n" +
                    "(52, 'Rugby, Warwickshire', 164.5038),\n" +
                    "(53, 'Sutton Coldfield, West Midlands', 164.3465),\n" +
                    "(54, 'Harlow, Essex', 205.7882),\n" +
                    "(56, 'Swansea', 244.845),\n" +
                    "(57, 'Chesterfield, Derbyshire', 182.6668),\n" +
                    "(58, 'Lisburn, Ireland', 386.795),\n" +
                    "(59, 'Londonderry, Derry', 477.334),\n" +
                    "(60, 'Salisbury, Wiltshire', 214.9566),\n" +
                    "(61, 'Manchester', 197.1591),\n" +
                    "(62, 'Bristol', 203.4813),\n" +
                    "(63, 'Wolverhampton, West Midlands', 168.1432),\n" +
                    "(64, 'Preston, Lancashire', 220.8832),\n" +
                    "(66, 'Hastings, East Sussex', 270.9555),\n" +
                    "(67, 'Bedford', 181.1172),\n" +
                    "(68, 'Basildon, Essex', 226.964),\n" +
                    "(69, 'Chippenham, Wiltshire', 194.1616),\n" +
                    "(70, 'Birmingham, West Midlands', 164.5807),\n" +
                    "(71, 'Folkestone, Kent', 283.8043),\n" +
                    "(73, 'Southampton', 225.356),\n" +
                    "(74, 'Belfast, Northern Ireland', 387.4128),\n" +
                    "(75, 'Uckfield, East Sussex', 245.1897),\n" +
                    "(76, 'Worthing, West Sussex', 243.29),\n" +
                    "(77, 'Leeds, West Yorkshire', 212.8972),\n" +
                    "(78, 'Kendal, Cumbria', 261.3619),\n" +
                    "(79, 'Hull', 233.4404),\n" +
                    "(80, 'Plymouth', 326.9285),\n" +
                    "(81, 'Haverhill, Suffolk', 214.1269),\n" +
                    "(82, 'Frankton, Warwickshire', 163.8717)"
            );

        } catch (SQLException e) {
            System.err.println(e.getLocalizedMessage());
            System.out.println("WARNING: could not import data into table 'AVG_DISTANCE_SOLUTION'");
        }
    }

    @Before
    public void initTestCase() throws Exception {
        // connect to database
        con = DriverManager.getConnection(ConnectionConfig.DB2_URL + ConnectionConfig.DB2_DB, ConnectionConfig.DB2_USER, ConnectionConfig.DB2_PW);
        cleanTables(con);
        createAndPopulateBarsUk(con);
        createAndPopulatePlacesUk(con);
        createAndPopulateDistancePlacesSolution(con);
        createAndPopulateBarsWithAffiliationsSolution(con);
        createAndPopulateAvgDistanceSolution(con);
        studentInstance = (Exercise04Interface) Class.forName("de.tuberlin.dima.dbpra.exercises.Exercise04").getDeclaredConstructor().newInstance();
    }

    /*
     * Test functions of 1st exercise
     */
    @Test(timeout = 10000)
    public void testEx01CreateUDFs() {
        testCtr = 0;
        scale[testCtr] = 0;

        System.out.println("Testing exercise 1");

        boolean fail = false;
        try {
            studentInstance.ex01CreateUDFs(con);
        } catch (SQLException e) {
            System.out.println("Failure in SQL exercise 1.");
            System.out.println(e.getMessage());
            fail = true;
        }

        try {
            printTestStart("function 'HAVERSINE'");
            ResultSet res = con.createStatement().executeQuery("SELECT HAVERSINE(51.970390, 0.979328, 53.958332, -1.080278) from sysibm.sysdummy1");
            assertTrue(res.next());
            assertEquals(318.28684, res.getDouble(1), 0.0001);

            res = con.createStatement().executeQuery("SELECT HAVERSINE(52.54576427013963, 13.404058223302775, 37.80041352092808, 23.819097437384283) from sysibm.sysdummy1");
            assertTrue(res.next());
            assertEquals(1935.30018, res.getDouble(1), 0.0001);

            res = con.createStatement().executeQuery("SELECT HAVERSINE(53.680000, -1.490000, 52.328415, -1.377561) from sysibm.sysdummy1");
            assertTrue(res.next());
            assertEquals(150.76163, res.getDouble(1), 0.0001);

            scale[testCtr] += 0.5;
        } catch (SQLException e) {
            System.out.println("Test for function 'HAVERSINE' failed.");
            System.out.println(e.getMessage());
            fail = true;
        }

        try {
            printTestStart("function 'Round4'");
            ResultSet res = con.createStatement().executeQuery("SELECT ROUND4(327.12345678) from sysibm.sysdummy1");
            assertTrue(res.next());
            assertEquals(327.12346, res.getDouble(1), 0.0001);

            res = con.createStatement().executeQuery("SELECT ROUND4(0.4444444444) from sysibm.sysdummy1");
            assertTrue(res.next());
            assertEquals(0.44444, res.getDouble(1), 0.0001);

            res = con.createStatement().executeQuery("SELECT ROUND4(1.9999999) from sysibm.sysdummy1");
            assertTrue(res.next());
            assertEquals(2, res.getDouble(1), 0.0001);

            scale[testCtr] += 0.5;
        } catch (SQLException e) {
            System.out.println("Test for function 'Round4' failed.");
            System.out.println(e.getMessage());
            fail = true;
        }

        if (fail) {
            fail();
        } else {
            printSuccessful();
        }
    }

    @Test(timeout = 10000)
    public void testEx02CreateView1() {
        System.out.println("Testing exercise 2");
        testCtr = 1;
        scale[testCtr] = 0;

        printTestStart("view creation 'DISTANCE_PLACES'");
        try {
            studentInstance.ex01CreateUDFs(con);
            studentInstance.ex02CreateView1(con);
        } catch (SQLException e) {
            System.out.println("Failure in SQL exercise 2.");
            System.out.println(e.getMessage());
            fail();
        }

        ResultSet res, res_ref, res_update;
        try {
            // compare content, allow epsilon error
            res = con.createStatement().executeQuery("Select * from DISTANCE_PLACES order by NAME");
            res_ref = con.createStatement().executeQuery("Select * from DISTANCE_PLACES_SOLUTION order by NAME");
            try {
                while (res_ref.next()) {
                    res.next();
                    //compare distance
                    assertEquals(res.getDouble(5), res_ref.getDouble(5), .0001);
                }
            } catch (AssertionError e) {
                System.out.println("Content of DISTANCE_PLACES not as expected!");
                fail();
            }

            // change coordinates of London to test against hard coded coordinates
            runSQL("UPDATE PLACES_IN_UK t SET t.LATITUDE = 52.3, t.LONGITUDE = -0.1334 WHERE t.ID = 48",
                    "Updating table PLACES_IN_UK failed", debug);
            res_update = con.createStatement().executeQuery("Select * from DISTANCE_PLACES WHERE ID in (0, 1, 2) order by ID");
            try {
                while (res_update.next()) {
                    int id = res_update.getInt("ID");
                    double distLondon = res_update.getDouble("DISTANCE_LONDON");
                    if (id == 0) {
                        assertEquals(distLondon, 194.9149, 0.0001);
                    } else if (id == 1) {
                        assertEquals(distLondon, 142.56072, 0.0001);
                    } else if (id == 2) {
                        assertEquals(distLondon, 159.59251, 0.0001);
                    }
                }
            } catch (AssertionError e) {
                System.out.println("Content of DISTANCE_PLACES not as expected during London update!");
                fail();
            }
        } catch (SQLException e) {
            System.out.println("Error with student code for exercise 2");
            e.printStackTrace();
            fail();
        }

        scale[testCtr] = 1;
        printSuccessful();
    }

    @Test(timeout = 10000)
    public void testEx03CreateView2() {
        System.out.println("Testing exercise 3");
        testCtr = 2;
        scale[testCtr] = 0;

        printTestStart("view creation 'AVG_DISTANCE'");
        try {
            studentInstance.ex01CreateUDFs(con);
            studentInstance.ex02CreateView1(con);
            studentInstance.ex03CreateView2(con);
        } catch (SQLException e) {
            System.out.println("Failure in SQL exercise 3.");
            System.out.println(e.getMessage());
            fail();
        }

        ResultSet res, res_ref, res_update;
        try {
            // compare content, allow epsilon error
            res = con.createStatement().executeQuery("Select * from AVG_DISTANCE order by ID");
            res_ref = con.createStatement().executeQuery("Select * from AVG_DISTANCE_SOLUTION order by ID");
            try {
                while (res_ref.next()) {
                    res.next();
                    //compare distance
                    assertEquals(res.getDouble(3), res_ref.getDouble(3), .0001);
                }
            } catch (AssertionError e) {
                System.out.println("Content of AVG_DISTANCE not as expected!");
                fail();
            }

            // change coordinates of Leicester to test against hard coded coordinates
            runSQL("UPDATE PLACES_IN_UK t SET t.LATITUDE = 52.3, t.LONGITUDE = -0.1334 WHERE t.ID = 20",
                    "Updating table PLACES_IN_UK failed", debug);
            res_update = con.createStatement().executeQuery("Select * from AVG_DISTANCE WHERE ID in (0, 1, 2) order by ID");
            try {
                while (res_update.next()) {
                    int id = res_update.getInt("ID");
                    double avgDist = res_update.getDouble("AVG_DISTANCE");
                    if (id == 0) {
                        assertEquals(avgDist, 229.7165, 0.0001);
                    } else if (id == 1) {
                        assertEquals(avgDist, 172.4044, 0.0001);
                    } else if (id == 2) {
                        assertEquals(avgDist, 214.0378, 0.0001);
                    }
                }
            } catch (AssertionError e) {
                System.out.println("Content of AVG_DISTANCE not as expected during Leicester update!");
                fail();
            }
        } catch (SQLException e) {
            System.out.println("Error with student code for exercise 3");
            e.printStackTrace();
            fail();
        }

        scale[testCtr] = 1;
        printSuccessful();
    }

    @Test(timeout = 10000)
    public void testEx04CreateTrigger() {
        System.out.println("Testing exercise 4");

        testCtr = 3;
        scale[testCtr] = 0;

        printTestStart("trigger creation 'DISTANCE_PLACES_UPDATE'");

        try {
            studentInstance.ex01CreateUDFs(con);
            studentInstance.ex02CreateView1(con);
            runSQL("GRANT ALL ON DISTANCE_PLACES TO USER " + ConnectionConfig.DB2_USER, "There is no existing view DISTANCE_PLACES", debug);
            studentInstance.ex03CreateView2(con);
            runSQL("GRANT ALL ON AVG_DISTANCE TO USER " + ConnectionConfig.DB2_USER, "There is no existing view AVG_DISTANCE", debug);
            studentInstance.ex04CreateTrigger(con);
        } catch (SQLException e) {
            System.out.println("Error with student code for exercise 4");
            System.out.println(e.getMessage());
            fail();
        }

        for (int i = 0; i <= 5; i++) {
            runSQL("UPDATE DISTANCE_PLACES t SET t.LONGITUDE = " + (i * -1) + "," +
                            "t.LATITUDE= " + (i + 50) + " WHERE t.ID = " + i,
                    "Updating view DISTANCE_PLACES failed", debug);
        }

        ResultSet res;
        try {
            // compare content, allow epsilon error
            res = con.createStatement().executeQuery("Select * from PLACES_IN_UK WHERE ID in (0, 1, 2, 3, 4, 5) order by ID");
            try {
                while (res.next()) {
                    res.next();
                    int id = res.getInt("ID");
                    double lat = res.getDouble("LATITUDE");
                    double lon = res.getDouble("LONGITUDE");
                    assertEquals(lon, id * -1, 0.0);
                    assertEquals(lat, id + 50, 0.0);
                }
            } catch (AssertionError e) {
                System.out.println("Content of PLACES_IN_UK not as expected!");
                fail();
            }
        } catch (SQLException e) {
            System.out.println("Error with student code for exercise 4");
            e.printStackTrace();
            fail();
        }

        scale[testCtr] = 1;
        printSuccessful();
    }

    @Test(timeout = 10000)
    public void testEx05CreateProcedure() {
        System.out.println("Testing exercise 5");

        testCtr = 4;
        scale[testCtr] = 0;
        printTestStart("procedure ComputeAffiliationsOfBars");

        try {
            studentInstance.ex01CreateUDFs(con);
            studentInstance.ex02CreateView1(con);
            runSQL("GRANT ALL ON DISTANCE_PLACES TO USER " + ConnectionConfig.DB2_USER, "There is no existing view DISTANCE_PLACES", debug);
            studentInstance.ex03CreateView2(con);
            runSQL("GRANT ALL ON DISTANCE_PLACES TO USER " + ConnectionConfig.DB2_USER, "There is no existing view DISTANCE_PLACES", debug);
            studentInstance.ex04CreateTrigger(con);
            con.createStatement().executeUpdate("CREATE TABLE BARS_WITH_AFFILIATIONS(ID INTEGER NOT NULL, BARNAME VARCHAR(50), PLACENAME VARCHAR(50), PRIMARY KEY(ID))");
        } catch (SQLException e) {
            fail("ERROR: creation of table 'BARS_WITH_AFFILIATIONS' failed!");
        }

        try {
            // run student code
            studentInstance.ex05CreateProcedure(con);
            con.createStatement().executeUpdate("CALL ComputeAffiliationsOfBars()");
        } catch (SQLException e) {
            System.out.println("Error with student code for exercise 5");
            System.out.println(e.getMessage());
            fail();
        }

        ResultSet res, res_ref;
        try {
            // compare content, allow epsilon error
            res = con.createStatement().executeQuery("Select * from BARS_WITH_AFFILIATIONS order by ID");
            res_ref = con.createStatement().executeQuery("Select * from BARS_WITH_AFFILIATIONS_SOLUTION order by ID");
            try {
                while (res_ref.next()) {
                    res.next();
                    assertEquals(res.getString(3), res_ref.getString(3));
                }
            } catch (AssertionError e) {
                System.out.println("Content of table 'BARS_WITH_AFFILIATIONS' not as expected!");
                fail();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }

        scale[testCtr] = 1;
        printSuccessful();
    }

    private void runSQL(String sql, String errorMessage, boolean debug) {
        try {
            con.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            if (debug) {
                System.out.println("Debug message (no problem with solution):");
                System.out.println(errorMessage);
                System.out.println(e.getMessage());
            }
        }
    }

    @After
    public void cleanUp() throws Exception {
        int outCtr = testCtr + 1;
        System.out.println("+" + format.format(scale[testCtr] * points[testCtr]) + " for exercise " + outCtr + "\n");
        con.close();
    }

    private void printSuccessful() {
        System.out.println("... and passed successfully.");
    }

}
