package dis.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service for handling CSV file operations.
 */
@Service
public class CsvService {

    private static final Logger logger = LoggerFactory.getLogger(CsvService.class);

    /**
     * Read a CSV file and return its rows as a list of strings.
     *
     * @param filePath the path to the CSV file
     * @return list of strings, each representing a row in the CSV file
     */
    public List<String> readCsvFile(String filePath) {
        logger.info("Reading CSV file: {}", filePath);

        List<String> rows = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                rows.add(line);
            }
            logger.info("Successfully read {} rows from CSV file: {}", rows.size(), filePath);
        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", e.getMessage(), e);
        }
        rows.remove(0);

        return rows;
    }

    /**
     * Find a file ending with "_players.csv" in the given folder and read its rows.
     *
     * @param folderPath the path to the folder containing the CSV file
     * @return list of strings, each representing a row in the CSV file, or empty list if no file found
     */
    public List<String> readPlayersFile(String folderPath) {
        logger.info("Looking for players file in folder: {}", folderPath);

        File folder = new File(folderPath);
        if (!folder.exists() || !folder.isDirectory()) {
            logger.error("Folder does not exist or is not a directory: {}", folderPath);
            return new ArrayList<>();
        }

        File[] files = folder.listFiles((dir, name) -> name.endsWith("_players.csv"));
        if (files == null || files.length == 0) {
            logger.error("No file ending with '_players.csv' found in folder: {}", folderPath);
            return new ArrayList<>();
        }

        // Use the first matching file
        File playersFile = files[0];
        logger.info("Found players file: {}", playersFile.getAbsolutePath());
        
        return readCsvFile(playersFile.getAbsolutePath());
    }
}