package com.examples.scart.product;

import javax.servlet.http.HttpServletRequest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class ProductServiceMain {

	public static void main(String[] args) {
		SpringApplication.run(ProductServiceMain.class, args);
	}

	@GetMapping("/")
	public String home(HttpServletRequest req) {
		return "Welcome to Product Service API :: " + req.getRequestURL().toString();
	}

}
