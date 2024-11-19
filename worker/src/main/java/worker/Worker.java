package worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.sql.*;
import org.json.JSONObject;

class Worker {
  public static void main(String[] args) {
    try {
      Jedis redis = connectToRedis("redis");
      Connection dbConn = connectToDB("db");

      System.err.println("Watching vote queue");

      while (true) {
        String voteJSON = redis.blpop(0, "votes").get(1);
        JSONObject voteData = new JSONObject(voteJSON);
        String voterID = voteData.getString("voter_id");
        String CT_java = voteData.getString("CT");
        String ST_java = voteData.getString("ST");
        String TP_java = voteData.getString("TP");
        String LP_java = voteData.getString("LP");
        String LPE_java = voteData.getString("LPE");
        String SEE_java = voteData.getString("SEE");
        double CT_double = Double.parseDouble(CT_java);
        double ST_double = Double.parseDouble(ST_java);
        double TP_double = Double.parseDouble(TP_java);
        double LP_double = Double.parseDouble(LP_java); 
        double LPE_double = Double.parseDouble(LPE_java);
        double SEE_double = Double.parseDouble(SEE_java);
        double tce=CT_double+ST_double+TP_double;
        double lpw=LP_double+LPE_double;
        double final_score_double =(tce*0.4)+(SEE_double*0.8)+(lpw*0.1); 
        String final_score = String.valueOf(final_score_double);
        System.err.printf("Processing java CT for '%s' by '%s'\n", CT_java, voterID);
        System.err.printf("Processing java ST for '%s' by '%s'\n", ST_java, voterID);
        System.err.printf("Processing java TP for '%s' by '%s'\n", TP_java, voterID);
        System.err.printf("Processing java LP for '%s' by '%s'\n", LP_java, voterID);
        System.err.printf("Processing java LPE for '%s' by '%s'\n", LPE_java, voterID);
        System.err.printf("Processing java SEE for '%s' by '%s'\n", SEE_java, voterID);
        System.err.printf("Final Score %s\n", final_score, voterID);
        updateVote(dbConn, voterID, final_score);
        System.err.printf("DB Updated");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }


  

  static void updateVote(Connection dbConn, String voterID, String final_score) throws SQLException {
    PreparedStatement insert = dbConn.prepareStatement(
      "INSERT INTO votes (id, Final_Score) VALUES (?, ?)");
    insert.setString(1, voterID);
    insert.setString(2, final_score);
    try {
      insert.executeUpdate();
    } catch (SQLException e) {
      PreparedStatement update = dbConn.prepareStatement(
        "UPDATE votes SET Final_Score = ? WHERE id = ?");
      update.setString(1, final_score);
      update.setString(2, voterID);
      update.executeUpdate();
    }
  }

  static Jedis connectToRedis(String host) {
    Jedis conn = new Jedis(host);

    while (true) {
      try {
        conn.keys("*");
        break;
      } catch (JedisConnectionException e) {
        System.err.println("Waiting for redis");
        sleep(1000);
      }
    }

    System.err.println("Connected to redis");
    return conn;
  }

  static Connection connectToDB(String host) throws SQLException {
    Connection conn = null;

    try {

      Class.forName("org.postgresql.Driver");
      String url = "jdbc:postgresql://" + host + "/postgres";

      while (conn == null) {
        try {
          conn = DriverManager.getConnection(url, "postgres", "postgres");
        } catch (SQLException e) {
          System.err.println(e.getMessage());
          System.err.println("Waiting for db mkkmkm");
          sleep(1000);
        }
      }

      System.err.println("Will execute now");
      PreparedStatement drop_table = conn.prepareStatement("DROP TABLE IF EXISTS votes");
      drop_table.executeUpdate();
     
      PreparedStatement st = conn.prepareStatement(
        "CREATE TABLE IF NOT EXISTS votes (id VARCHAR(255) NOT NULL UNIQUE, Final_Score VARCHAR(255))");
      st.executeUpdate();

    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.err.println("Connected to db");
    return conn;
  }

  static void sleep(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      System.exit(1);
    }
  }
}
