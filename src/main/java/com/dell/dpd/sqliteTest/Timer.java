package com.dell.dpd.sqliteTest;

public class Timer {

	private long start;
	private int seconds;
	
	public Timer(){
		start = System.currentTimeMillis();
		seconds = -1; // timer still running
	}
	
	public void stop(){
		seconds = (int)(System.currentTimeMillis() - start) / 1000;
	}

	public void click(String format){
		System.out.printf(format + " ran for %d seconds.\n", seconds()) ;
	}

	public void end(String format){
		stop();
		click(format);
	}
	
	public void endMS(String format){
		long ms = System.currentTimeMillis() - start;
		System.out.printf(format + " ran for %d ms\n", ms);
	}
	
	public int seconds(){
		if(seconds > 0)
			return(seconds);
		return((int)(System.currentTimeMillis() - start) / 1000);
	}
}
