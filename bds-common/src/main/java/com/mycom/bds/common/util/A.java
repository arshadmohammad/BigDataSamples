package com.mycom.bds.common.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class A {

	private static final boolean IBM_JAVA = false;

	public static void main(String[] args) throws Exception{
		System.out.println(getDefaultRealm());
	}

	public static String getDefaultRealm() throws ClassNotFoundException, NoSuchMethodException,
			IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Object kerbConf;
		Class<?> classRef;
		Method getInstanceMethod;
		Method getDefaultRealmMethod;
		if (IBM_JAVA) {
			classRef = Class.forName("com.ibm.security.krb5.internal.Config");
		} else {
			classRef = Class.forName("sun.security.krb5.Config");
		}
		getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
		kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
		getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm", new Class[0]);
		return (String) getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
	}

}
