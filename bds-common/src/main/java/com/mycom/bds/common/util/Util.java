package com.mycom.bds.common.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private final static String ABC="Value1";

	public static void printTime() {
		System.out.println(getTime());
	}

	public static String getTime() {
		return dateFormat.format(new Date());
	}
	
	private class MyClass
	{
		
	   public void sayHi()
	   {
		   System.out.println("Hi");
	   }
		
	}
}
