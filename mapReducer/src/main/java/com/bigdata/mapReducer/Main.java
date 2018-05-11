package com.bigdata.mapReducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Main {
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "/");
		new HttpListener().initMapReduce();
		//SpringApplication.run(Main.class, args);
	}
}
