package com.dell.dpd.sqliteTest;

public class HashPart {

	private static int HASH_MAX = 65536;
	private static short[] hash = new short[HASH_MAX];	// using bits the hash can hold 2^32 values
	
	private int index;
	private int bitmask;
	
	public HashPart(){
		for(int i = 0 ; i < HASH_MAX ; i++){
			hash[i] = 0;
		}
	}
	
	public boolean isPresent(byte[] b, int offset){
		computeHash(b, offset);
		return (hash[index] & bitmask) > 0;
	}
	
	public void add(){
		hash[index] |= bitmask;
	}
	
	// each element is 6 bits (0:63)
	private void computeHash(byte b[], int position){
		int i1 = ((int)b[position] << 10);
		int i2 = ((int)b[position + 1] << 4);
		int i3 = ((int)b[position + 2] & 0x0f) ;

		index = ((int)b[position] << 10) + ((int)b[position + 1] << 4) + ((int)b[position + 2] & 0x0f) ;
		bitmask = 1 << ((int)b[position + 3] & 0x0F);
	}
}
