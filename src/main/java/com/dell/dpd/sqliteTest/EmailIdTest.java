package com.dell.dpd.sqliteTest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

// Test the performance of loading and finding string based Ids in SqlLite
// Each ID is 152 characters long.  Each character is in [a-zA-0-9]
public class EmailIdTest
{

// main function. Project run starts from main function...
// O365 Id2 are 152 bytes long and consist of [a-z-A-0-9-=]
	static public byte[] charSet = new byte[64];
	static public int IdLength = 152;
	
// 7 years of daily backups
	public int days;

	// 10 GB mailbox is about 70K emails (150K avg size)
	public int initialSize;
	public int incrementalSize;
	
	static public int IdMax;
	
	static public byte[][] IdList;
	
	public EmailIdTest(int initialSize, int incrementalSize, int iterations){
		this.initialSize = initialSize;
		this.incrementalSize = incrementalSize; 
		days = iterations;
		IdMax = (int) (initialSize + incrementalSize * days); //1850000;
		IdList = new byte[IdMax][IdLength];
		
		initCharSet();	
	}
	
   public void go() {
   	
	   Hash hash = new Hash();
	  		
	   // Generate a list of 1.85M O365 IDs to represent 7 years of daily backups - each one is 152 characters long
	   for(int i = 0 ; i < IdMax ; i++){
	   	createId(i, hash);
	   }   	
		System.out.printf("%d Ids created\n", IdMax);
		
		int base = 25236;		// random id to dump out for testing
		for(int i = 0 ; i < 25 ; i++)
			System.out.printf("Id %d  %s\n", base + i, new String(IdList[base + i]));
				
		Sqlite db = new Sqlite("C://O365/O365.db");

		// clean up any existing data
		db.executeStatement("DELETE FROM tblIds");
		db.executeStatement("DROP INDEX idx");

		
		// Simulate day0 backup - 1 transaction
		Timer t1 = new Timer();
		db.setCommitMode(false);
		for(int i = 0 ; i < initialSize ; i++){
			db.insertEmail(IdList[i], i, i * 10);
		}
		db.commit();
		t1.click("Initial load");
		
		// simulate 7 years of daily incrementals - 1 transaction per day
		int index = initialSize;
		int lastOutputSecond = 0;
		Timer t = new Timer();
		for(int i = 0 ; i < days ; i++){
			for(int j = 0 ; j < incrementalSize ; j++){
				db.insertEmail(IdList[index++], i * j, i * j * 10);
			}
			db.commit();
			if(t.seconds() % 5 == 0){
				if(lastOutputSecond != t.seconds()){
					lastOutputSecond = t.seconds();
					t.click(String.format("Loading of day %d", i));
				}
			}
		}   	
		t1.end("Data loading");
		
		
		// single query time
		t1 = new Timer();
		db.queryEmailId(new String(IdList[base]));
		t1.end("Query");
		
		// how many queries in 10 seconds?
		queryTest(db, "Unindexed");

		t1 = new Timer();
//		db.openConnection();
		db.index("idx", "TblIds",  "id");
		db.commit();
		t1.end("Indexing");

		queryTest(db, "Indexed");
   }

   
   public static void queryTest(Sqlite db, String msg){
		Random rand = new Random();
		int i = 0;
		for(Timer t1 = new Timer(); t1.seconds() < 10; i++){
			db.queryEmailId(new String(IdList[rand.nextInt(IdMax)]));
			if(i % 25 == 0) System.out.printf(".");
		}
		System.out.printf("\nExecuted %s %d ID queries over 10 seconds (%2.1f / sec) \n", msg, i, (double)i / 10.0);
   }

   
   static int dups = 0;
   static int last = -1;
   private static void createId(int index, Hash h){
   		
	   	Random rand = new Random();
	   	int v;
	   	byte[] vList = new byte[IdLength];
	   	byte[] id = new byte[IdLength];
	   	for(int j = 0 ; j < IdLength ; j++){
	   		v = rand.nextInt(64);
	   		vList[j] = (byte)v;			// hold onto raw value for hash/dup detection
	   		id[j] = (byte)charSet[v];
	   	}
	   	if(!h.isPresent(vList)){
	   		h.add();
	   		IdList[index] = id;
	   	}
	   	else{
	   		if(last != index){
	   			last = index;
	   			dups = 0;
	   		}
	   		if(++dups % 50 == 0)
	   			System.out.printf("%d Duplicate id at index %d\n",  dups, index);
	   		createId(index, h);
	   	}
   }
   	
   
   private static void initCharSet(){
	   	// init the possible set of characters
	   	for(int i = 0 ; i < 26 ; i++ ){
	   		charSet[i] = (byte)('A' + i);
	   		charSet[i+26] = (byte)(charSet[i] + 32);
	   	}
	   	for(int i = 0 ; i < 10 ; i++)
	   		charSet[i+52] = (byte)('0' + i);
	
	   	charSet[62] = (byte)'=';
	   	charSet[63] = (byte)'-';

   }

}
