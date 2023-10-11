import java.io.*;
import java.sql.*;
import java.util.*;

public class foo {
  static Connection connection = null;
  
    

    public static void main(String[] args) throws SQLException {


        String user ="zjac249";
        String password = "ahngee";
        String database = "localhost";
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter Username here: ");
        user = sc.nextLine();
        System.out.println("Enter Password here: ");
        password = sc.nextLine();
        
        Connection connection = connectToDatabase(user, password, database);
        if (connection != null) {
            System.out.println("SUCCESS: You made it!"
                    + "\n\t You can now take control of your database!\n");
        } else {
            System.out.println("ERROR: \tFailed to make connection!");
            System.exit(1);
        }
        dropTable(connection, "airport");
        createTable(connection,
            "airport (airportCode varchar(4), airportName varchar(160), City varchar(130), State varchar(2), PRIMARY KEY (airportCode));");
        dropTable(connection, "delayedFlights");
        createTable(connection,"delayedFlights (FlightID int, Month int, DayOfMonth int, DayOfWeek int, DepTime int, "
                + "ScheduledDepTime int, ArriveTime int, ScheduledArrTime int, UniqueCarrier char(2), "
                + "FlightNum int, ActualFlightTime int, ScheduledFlightTime int, AirTime int, "
                + "ArriveDelay int, depDelay int, OrigDep varchar(3), Dest varchar(3), Distance int, PRIMARY KEY (FlightID), "
                + "FOREIGN KEY (OrigDep) REFERENCES airport(airportCode), "
                + "FOREIGN KEY (Dest) REFERENCES airport(airportCode));");
        InsertTableAirport(connection, "src/airport");
        InsertTableDelayedFlights(connection, "src/delayedFlights");
        
        String query1;  
        query1 = "SELECT UniqueCarrier, COUNT(UniqueCarrier) AS total "
            + "FROM delayedFlights "
            + "GROUP BY UniqueCarrier "
            + "ORDER BY total DESC "
            + "LIMIT 5";
        
        String query2;
        query2 =  "SELECT City, COUNT(OrigDep) AS total "
            + "FROM delayedFlights, airport "
            + "WHERE OrigDep = AirportCode "
            + "GROUP BY City "
            + "ORDER BY total DESC "
            + "LIMIT 5";
        
        String query3;
        query3 = "SELECT delayedFlights.Dest, MAX(delaytotal.total) AS total "
            + "FROM delayedFlights, (SELECT delayedFlights.Dest, SUM(ArriveDelay) as total "
            + "FROM delayedFlights "
            + "GROUP BY delayedFlights.Dest "
            + "ORDER BY total DESC)delaytotal "
            + "WHERE (delayedFlights.Dest = delaytotal.Dest) " 
            + "GROUP BY delayedFlights.Dest "
            + "ORDER BY total DESC " + "LIMIT 5" + " OFFSET 1";
        
        String query4;
        query4 = "SELECT airport1.State, COUNT(airport1.State) AS total "
            + "FROM airport as airport1, airport as airport2, delayedFlights "
            + "WHERE delayedFlights.OrigDep = airport1.airportCode AND airport1.State = airport2.State AND delayedFlights.Dest = airport2.airportCode "
            + "GROUP BY airport1.state "
            + "ORDER BY total DESC "
            + "LIMIT 5";
        System.out.println("################## 1st Query ###############");
        DisplayQueries(executeQuery(connection, query1));
        System.out.println("################## 2nd Query ###############");
        DisplayQueries(executeQuery(connection,query2));
        System.out.println("################## 3rd Query ###############");
        DisplayQueries(executeQuery(connection,query3));
        System.out.println("################## 4th Query ###############");
        DisplayQueries(executeQuery(connection, query4));
        

        sc.close();
    }
    
          public static void InsertTableDelayedFlights(Connection connection, String filename) {
            String currentLine = null;
            try {
                BufferedReader br = new BufferedReader(new FileReader(filename));
                Statement st = connection.createStatement();
    
            while ((currentLine = br.readLine()) != null) {
          String[] values = currentLine.split(",");
          String line = "INSERT INTO " + "delayedFlights" + " VALUES (" + values[0] + ","
              + values[1] + "," + values[2] + "," + values[3] + "," + values[4] + "," + values[5]
                  + "," + values[6] + "," + values[7] + ",'" + values[8] + "'," + values[9] + ","
                  + values[10] + "," + values[11] + "," + values[12] + "," + values[13] + "," + values[14]
                  + ",'" + values[15] + "','" + values[16] + "'," + values[17] + ");";
          st.executeUpdate(line);
}
            br.close();
          } catch (Exception e) {
          e.printStackTrace();
          }
          
          }
          public static void InsertTableAirport(Connection connection, String filename) {        
            String currentLine = null;
            try {
                BufferedReader br = new BufferedReader(new FileReader(filename));
                Statement statement = connection.createStatement();
    
            while ((currentLine = br.readLine()) != null) {
          String[] values = currentLine.split(",");
          String line  = "INSERT INTO " + "airport" + " VALUES ('" + values[0] + "','"
              + values[1] + "','" + values[2] + "','" + values[3] + "');";
    
    
          statement.executeUpdate(line);
            }
            br.close();
          } catch (Exception e) {
          e.printStackTrace();
          }
          
          }
          
        public static ResultSet executeQuery(Connection connection, String query) {
           System.out.println(" Executing query...");
           try {
           Statement st = connection.createStatement();
           ResultSet rs = st.executeQuery(query);
           return rs;
           } catch (SQLException e) {
           e.printStackTrace();
           return null;
           }
           }
        
        public static void createTable(Connection connection, String tableDescription) throws SQLException {
          Statement stmt = connection.createStatement();
          stmt = connection.createStatement();
          stmt.execute("CREATE TABLE " + tableDescription);
          stmt.close();
        }
          
          public static void dropTable(Connection connection, String table) throws SQLException {
            Statement stmt = connection.createStatement();
            stmt.execute("DROP TABLE IF EXISTS " + table + " CASCADE;");
          }
            

          public static void DisplayQueries(ResultSet rs) throws SQLException {
              while (rs.next()) {
                System.out.println(rs.getString(1) + " " + rs.getString(2));
              }
            } 
            
            
            
          

        public static Connection connectToDatabase(String user, String password, String database) {
          System.out.println("------ Testing PostgreSQL JDBC Connection ------");
          Connection connection = null;
          try {
              String protocol = "jdbc:postgresql://";
              String dbName = "/CS2855/";
              String fullURL = protocol + database + dbName + user;
              connection = DriverManager.getConnection(fullURL, user, password);
          } catch (SQLException e) {
              String errorMsg = e.getMessage();
              if (errorMsg.contains("authentication failed")) {
                  System.out.println("ERROR: \tDatabase password is incorrect. Have you changed the password string above?");
                  System.out.println("\n\tMake sure you are NOT using your university password.\n"
                          + "\tYou need to use the password that was emailed to you!");
              } else {
                  System.out.println("Connection failed! Check output console.");
                  e.printStackTrace();
              }
          }
          return connection;
        }
}
        