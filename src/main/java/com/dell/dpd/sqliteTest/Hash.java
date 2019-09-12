package com.dell.dpd.sqliteTest;

public class Hash {


	private static int NUM_HASHES = 3;
	private static HashPart[] hashParts = new HashPart[NUM_HASHES];
	
	public Hash(){
		for(int i = 0 ; i < NUM_HASHES ; i++)
			hashParts[i] = new HashPart();
	}
	
	public boolean isPresent(byte[] b){
		for(int i = 0 ; i < NUM_HASHES ; i++)
			if(!hashParts[i].isPresent(b, i * 5))
				return false;
		return true;
	}
	
	public void add(){
		for(int i = 0 ; i < NUM_HASHES ; i++)
			hashParts[i].add();
	}
}
