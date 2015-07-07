package com.database_preservation;

import org.testng.annotations.*;

public class PostgreSqlTest {

	 @BeforeGroups(groups={"mysql"})
	 public void setUp() {
		 System.out.println("setting up...");
	 }

	 @AfterGroups(groups={"mysql"})
	 public void tearDown(){
		 System.out.println("tearing down...");
	 }

	 @Test(groups = { "mysql" })
	 public void firstTest() {
	   System.out.println("mysql test");
	 }
}
