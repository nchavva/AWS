package com.csv.parser;


import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

@SpringBootApplication
public class ParserApplication {

	public static void main(String[] args) {
		SpringApplication.run(ParserApplication.class, args);
	}

	//@Autowired
	//private CustomerRepository repository;

	@Value("${spring.jpa.properties.hibernate.jdbc.batch_size}")
	private int batchSize;

	@Value("${csv.location}")
	private String file;

	@Value("${spring.datasource.url}")
	private String url;

	@Value("${spring.datasource.username}")
	private String user;

	@Value("${spring.datasource.password}")
	private String pwd;

	private static final String SQL_INSERT = "INSERT INTO customers (customer_id, customer_name, city) VALUES (?,?,?)";

	@EventListener(ApplicationReadyEvent.class)
	public void doSomethingAfterStartup() {

		try (Connection conn = DriverManager.getConnection(url,user,pwd);
			 PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT)
			 ) {


			// create csvReader object passing
			// file reader as a parameter
			CSVReader csvReader = new CSVReader(new FileReader(file));
			String[] csvrow =null;
			int recordNo = 0;

			int insrows[];
			// we are going to read data line by line
			while ((csvrow  = csvReader.readNext()) != null) {
				recordNo = recordNo+1;
				psInsert.setLong(1, recordNo);
				psInsert.setString(2, csvrow[1]);
				psInsert.setString(3, csvrow[2]);
				psInsert.addBatch();
				if(recordNo % 1000 ==0)
				{
					 insrows = psInsert.executeBatch();
					System.out.println("total inserted rows" + insrows.length);
				}
			}
			if(recordNo % 1000 !=0)
			{
				 insrows = psInsert.executeBatch();
				System.out.println("total inserted rows" + insrows.length);
			}
			//conn.commit();
			System.out.println("insertion done");
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}
}