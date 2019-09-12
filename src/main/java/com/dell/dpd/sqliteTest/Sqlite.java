package com.dell.dpd.sqliteTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



public class Sqlite {
	private static String dbName;
	private static Connection conn = null;
	private static PreparedStatement pstmt = null;
	private static boolean autoCommit = false;

	Sqlite(String dbName){
		Sqlite.dbName = dbName;
	}
	
	 public void setCommitMode(boolean autoCommit){
		 Sqlite.autoCommit = autoCommit;
	 }
	 
	 private Connection openConnection() {
       // SQLite connection string
       if(conn == null){
      	 
			 String url = "jdbc:sqlite:" + dbName;
	       try {
	           conn = DriverManager.getConnection(url);
	           conn.setAutoCommit(autoCommit);
	       } catch (SQLException e) {
	           System.out.println(e.getMessage());
	           e.printStackTrace();
	       }
       }
       return conn;
   }
	 

	 public void closeConnection(){
			pstmt = null;
			if(conn != null){
				try {
					conn.commit();
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				conn = null;
			}
	 }

	 public void commit(){
		 closeConnection();
		 openConnection();
	}
	 

   public void insertEmail(byte[] b, int offset, int len) {
      String sql = "INSERT INTO tblIds(id, offset, length) VALUES(?, ?, ?)";

   	openConnection();

      try {
      		if(pstmt == null) 
      			pstmt = conn.prepareStatement(sql);
	      	pstmt.setBytes(1, b);
	      	pstmt.setInt(2,  offset);
	      	pstmt.setInt(3,  len);
         pstmt.execute();
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
   }
   
   public void insertBR(long timestamp, long dmask1, long dmask2, int bid, int cid) {
      String sql = "INSERT INTO tblBR(timestamp, dmask1, dmask2, bid, cid) VALUES(?, ?, ?, ?, ?)";

     	openConnection();

      try {
      		if(pstmt == null) 
      			pstmt = conn.prepareStatement(sql);
	      	pstmt.setLong(1, timestamp);
	      	pstmt.setLong(2, dmask1);
	      	pstmt.setLong(3, dmask2);
	      	pstmt.setInt(4, bid);
	      	pstmt.setInt(5, cid);
         pstmt.execute();
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
   }
   
   public void insertCatalog(int id, long createTime, long expireTime) {
      String sql = "INSERT INTO tblCatalog(id, createTime, expireTime) VALUES(?, ?, ?)";

     	openConnection();

      try {
      		if(pstmt == null) 
      			pstmt = conn.prepareStatement(sql);
      			pstmt.setInt(1,  id);
      			pstmt.setLong(2, createTime);
      			pstmt.setLong(3, expireTime);
      			pstmt.execute();
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
   }
   
   public void index(String name, String table, String column){
   String sql = String.format("CREATE UNIQUE INDEX %s ON %s(%s)", name, table, column);
   		executeStatement(sql);
   }

	public void executeStatement(String sql) {

     	commit();

		try {
			Statement stmt = conn.createStatement();
			stmt.execute(sql);
			commit();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public ResultSet executeQuery(String sql){
		ResultSet rs = null;

     	commit();

		try {
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return rs;
	}
   
   public boolean queryEmailId(String id){
   		boolean ret = false;

        	openConnection();

   		try {
	   		ResultSet rs = executeQuery("SELECT * FROM tblIds WHERE id like " + "\"%" + id + "%\"");
		  
		  // Check query results - any matches?
		  if (rs.next()) {
			  ret = true;
		  } 
		  rs.close();
	   }
	   catch(Exception e){
		   	System.out.println(e.getMessage());
	   }
	   	return ret;
   }
   
   public int copyBR(long oldTimestamp, long timestamp){
   	int count = 0;
   	int maxCid = 0;

   	try {
   		ResultSet rs  = executeQuery("SELECT MAX(cid) FROM tblBR WHERE timestamp = " + oldTimestamp);
   		if(rs.next())
   			maxCid = rs.getInt(1);
   		rs.close();
			
   		// loop through the result set
   		rs = executeQuery("SELECT * FROM tblBR WHERE timestamp = " + oldTimestamp);
   		while (rs.next()) {
   			count++;
   			if(rs.getInt(2) != -1)
   				insertBR(timestamp, rs.getInt(2),  rs.getInt(3),  rs.getInt(4) + 1,  rs.getInt(5));
   			else
   				System.out.printf("skipped record %d\n", count);
   		} 
   		rs.close();
   	}
   	catch(Exception e){
   		System.out.println(e.getMessage());
   	}
   	commit();
   	return maxCid;
   }

}
