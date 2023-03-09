package org.user.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UserDataProcessor {

	public static void main(String[] args) {
        // Read the file
		URL path = UserDataProcessor.class.getResource("file.txt");
		File userFile = new File(path.getFile());
		try (BufferedReader br = new BufferedReader(new FileReader(userFile))) {
			String line;
			ExecutorService pool = Executors.newFixedThreadPool(100);
			while ((line = br.readLine()) != null) {
				pool.execute(processRecord(line));
			}
			pool.shutdown();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	private static Runnable processRecord(String line) {
		Runnable task = () -> {
			try {
				// Parse the record
				User user = parseRecord(line);
				// Validate the record
				validateUser(user);
				// Save to user table
				saveUser(user);
				System.out.println("Success");
			} catch (ValidationExcetpion e) {
				// Save to error table.
				System.out.println("Failed");
				processError(line, e);
			}

		};
		return task;
	}

	private static void processError(String line, ValidationExcetpion e) {
		System.out.println(line + "   :: " + e.getMessage());
	}

	private static void saveUser(User user) {
		
	}

	private static void validateUser(User user)  throws ValidationExcetpion {
		String message = null;
		if (null == user.getFirstName() || user.getFirstName().length() == 0) {
			message = "First Name cannot be empty OR null";
		}
		if (null != message ) {
			throw new ValidationExcetpion(message);
		}
	}

	private static User parseRecord(String line) {
		System.out.println(line);
		// Convert to Object
		String[] values = line.split(",");
		User user = new User();
		user.setFirstName(values[0]);
		return user;
	}

}
