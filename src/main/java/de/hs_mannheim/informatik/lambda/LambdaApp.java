package de.hs_mannheim.informatik.lambda;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;

@SpringBootApplication
public class LambdaApp {


	public static void main(String[] args) {

		SpringApplication.run(LambdaApp.class, args);
		
		System.out.println("Ausführungsort: " + new File(".").getAbsolutePath());
	}
}
