package com.dell.dpd.sqliteTest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Random;
import java.util.UUID;

// Test the performance of loading and finding string based Ids in SqlLite
// Each ID is 152 characters long.  Each character is in [a-zA-0-9]
public class App
{

	
   public static void main(String[] args) throws IOException, SQLException {
   	
	   	// email Id testing
	   // 7 years of daily backups
	   	int days = 7 * 365;
	
	   	// 10 GB mailbox is about 70K emails (150K avg size)
	   	int mailboxSize = 2000; //70000;
	   	double changeRate = 0.01;

//   		EmailIdTest idTest = new EmailIdTest(mailboxSize, (int) (mailboxSize * changeRate), days);
//   		idTest.go();
   		
   		GcTest gc = new GcTest(mailboxSize, (int) (mailboxSize * changeRate), days);
   		gc.go();

   }

}
