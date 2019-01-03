package com.smattme.cassandra2sql;

import com.smattme.cassandra2sql.services.DatabaseService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DatabaseServiceIntegrationTest {

    private Logger logger = LoggerFactory.getLogger(DatabaseServiceIntegrationTest.class);


    @Test
    void givenPostgresPropertyFilePath_whenGenerateSQLFromKeySpace_thenPostgresCompatibleSQL() {
        String propertyFile = "postgres-application.properties";
        File file = new File(propertyFile);
        assumeTrue(file.exists(), propertyFile + " does not exist so test is skipped");
        DatabaseService databaseService = new DatabaseService(propertyFile);
        databaseService.generateSQLFromKeySpace();
        Map<String, String> generatedSQL = databaseService.getGeneratedSQL();
        File generatedZipFile = databaseService.getGeneratedZipFile();

        assertNotNull(generatedSQL);
        assertNotNull(generatedZipFile);

        logger.info("generated zip file: \n" + generatedZipFile.getAbsolutePath());
        logger.info("generated sql: \n" + generatedSQL);
    }

    @Test
    void givenMYSQLPropertyFilePath_whenGenerateSQLFromKeySpace_thenMySQLCompatibleSQL() {
        String propertyFile = "mysql-application.properties";
        File file = new File(propertyFile);
        assumeTrue(file.exists(), propertyFile + " does not exist so test is skipped");
        DatabaseService databaseService = new DatabaseService(propertyFile);
        databaseService.generateSQLFromKeySpace();
        Map<String, String> generatedSQL = databaseService.getGeneratedSQL();
        File generatedZipFile = databaseService.getGeneratedZipFile();

        assertNotNull(generatedSQL);
        assertNotNull(generatedZipFile);

        logger.info("generated zip file: \n" + generatedZipFile.getAbsolutePath());
        logger.info("generated sql: \n" + generatedSQL);
    }


    @Test
    void givenAlternatePropertyFilePath_whenGenerateSQL_thenDoNotPreserveGeneratedFiles() {
        String propertyFile = "application-no-preserve.properties";
        File file = new File(propertyFile);
        assumeTrue(file.exists(), propertyFile + " does not exist so test is skipped");
        DatabaseService databaseService = new DatabaseService(propertyFile);
        databaseService.generateSQLFromKeySpace();
        Map<String, String> generatedSQL = databaseService.getGeneratedSQL();
        File generatedZipFile = databaseService.getGeneratedZipFile();
        assertTrue(generatedSQL.isEmpty());
        assertFalse(generatedZipFile.exists());
    }


    @Test
    void givenPropertyFile_whenGenerateSQL_thenDoNotSendEmail() {
        String propertyFile = "application-no-email.properties";
        File file = new File(propertyFile);
        assumeTrue(file.exists(), propertyFile + " does not exist so test is skipped");
        DatabaseService databaseService = new DatabaseService("application-no-email.properties");
        databaseService.generateSQLFromKeySpace();
        Map<String, String> generatedSQL = databaseService.getGeneratedSQL();
        File generatedZipFile = databaseService.getGeneratedZipFile();
        assertNotNull(generatedSQL);
        assertNotNull(generatedZipFile);
    }

}
