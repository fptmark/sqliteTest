package com.dell.dpd.sqliteTest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// Test the performance of loading and finding string based Ids in SqlLite
// Each ID is 152 characters long.  Each character is in [a-zA-0-9]
public class GcTest
{

// 7 years of daily backups
	private int days;

	// 10 GB mailbox is about 70K emails (150K avg size)
	private int initialSize;
	private int incrementalSize;
	
	private Dmask dmask;
	private Random rand;
	
	private Sqlite db;
	
	public GcTest(int initialSize, int incrementalSize, int iterations){
		this.initialSize = initialSize;
		this.incrementalSize = incrementalSize; 
		days = iterations;
		dmask = new Dmask(0.1);	// 0.1 of all BR records will have a dmask of -1 = simulate invalidate 10% of all containers
				
	}
	
	public void go() throws SQLException{
		db = new Sqlite("C://O365//sqliteTest//O365.db");
		rand = new Random();
		load();
		expireBackups(1000);
	}
	
	private void expireBackups(int count){
		int backupCount = 0;
		int recNum;

		try{
			ResultSet rs = db.executeQuery("SELECT MAX(id) FROM tblCatalog");
			backupCount = rs.getInt(1);
			rs.close();
			
			for(int i = 0 ; i < count ; i++){
				// get a random catalog id in [2:max], look up the timestamp and delete the records in tblBR with that timestamp
				recNum = rand.nextInt(backupCount - 2) + 2;
				rs = db.executeQuery("SELECT * FROM tblCatalog WHERE id = " + recNum);
				if(rs.next()){
					Timer t = new Timer();
					expireBackup(rs.getInt(2));
					t.endMS("Delete backup ");
				}
				else
					i--;	// loop again because that backup is gone
				rs.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private void expireBackup(long timestamp){
		ResultSet rs = null;
		List<Integer> cid = new ArrayList<Integer>();
		
		rs = db.executeQuery("SELECT * FROM tblBR WHERE timestamp = " + timestamp);
		try {
			while(rs.next())
				cid.add(rs.getInt(5));
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
		
		db.executeStatement("DELETE FROM tblBR WHERE timestamp = " + timestamp);
		db.executeStatement("DELETE FROM tblCatalog WHERE createTime = " + timestamp);
		db.executeStatement("CREATE INDEX gc ON tblBR(dmask1, cid)");
		for (Integer id : cid){
			db.executeStatement("DELETE FROM tblBR WHERE dmask1 = -1 AND cid = " + id);
		}
		db.executeStatement("DROP INDEX gc");
	}
	
   private void load() throws SQLException {
//   	ResultSet rs = db.executeQuery("SELECT count(*) FROM tblCR");// WHERE timestamp = 1568063947866");
//   	int k = rs.getInt(1);
		// clean up any existing data
		db.executeStatement("DELETE FROM tblBR");
		db.executeStatement("DELETE FROM tblCatalog");
//		db.executeStatement("DROP INDEX idx-br");
		
		// Simulate day0 backup - 1 transaction
		int backupId = 0;
		long lastTimestamp = System.currentTimeMillis();
		db.setCommitMode(false);
		for(int i = 0 ; i < initialSize/1000 ; i++){
			long v = dmask.get(false);
			db.insertBR(lastTimestamp, v, v, 1, i + 1);
		}
		db.commit();
		db.insertCatalog(++backupId, lastTimestamp, lastTimestamp + 10000);
		db.commit();
		
		// simulate 7 years of daily incrementals - 1 transaction per day
		long maxInsertTime = 0;
		for(int i = 0 ; i < days ; i++){
			if(i % 10 == 0) 
				System.out.printf("Max insert time per day = %d ms at day %d\n", maxInsertTime, i);
			
			long timestamp = System.currentTimeMillis();
			int max = db.copyBR(lastTimestamp, timestamp);
			for(int j = 0 ; j < Math.ceil((double)incrementalSize/1000.0) ; j++){
				long v = dmask.get(true);
				db.insertBR(timestamp, v, v, i + 2, j + 1 + max);
			}
			db.commit();
			db.insertCatalog(++backupId, timestamp, timestamp + 10000);
			db.commit();
			lastTimestamp = timestamp;
			long t2 = System.currentTimeMillis();
			if(t2 - timestamp > maxInsertTime)
				maxInsertTime = t2 - timestamp;

			// after 200 days simulate 1 dead container per day
			int records = initialSize/1000 + (int)Math.ceil((double)incrementalSize/1000.0);
			if(i > 200){
				// find a record (bid, cid)
				// delete the record
				// search for other records with the same bid/cid
				
			}
		}   	
   }
   
}
