package com.dell.dpd.sqliteTest;

import java.util.Random;

public class Dmask {

	private Random rand;
	private int count;
	
	public Dmask(double percentage){
		this.count = (int)(1.0 / percentage);
		rand = new Random();
	}
	
	public long get(boolean generateMinusOne){
		int value = rand.nextInt(count);
		return value == 0 && generateMinusOne ? -1 : value;
	}
}
