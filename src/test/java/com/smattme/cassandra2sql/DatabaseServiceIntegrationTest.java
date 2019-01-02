package com.smattme.cassandra2sql;

import com.smattme.cassandra2sql.services.ConnectionService;
import com.smattme.cassandra2sql.services.DatabaseService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseServiceIntegrationTest {

    private Logger logger = LoggerFactory.getLogger(DatabaseServiceIntegrationTest.class);

    @AfterAll
    static void cleanUp() {
        ConnectionService.getInstance().closeConnection();
    }


    @Test
    void givenPropertyFilePath_whenGenerateSQLFromKeySpace_thenReadConfig() {

        DatabaseService databaseService = new DatabaseService("application.properties");
        databaseService.generateSQLFromKeySpace();
        Map<String, String> generatedSQL = databaseService.getGeneratedSQL();
        File generatedZipFile = databaseService.getGeneratedZipFile();

        assertNotNull(generatedSQL);
        assertNotNull(generatedZipFile);

//        logger.info("generated zip file: " + generatedZipFile.getAbsolutePath());
//        logger.info("generated sql: " + generatedSQL);
    }


    @Test
    void givenAlternatePropertyFilePath_whenGenerateSQL_thenDoNotPreserveGeneratedFiles() {
        DatabaseService databaseService = new DatabaseService("application-no-preserve.properties");
        databaseService.generateSQLFromKeySpace();
        Map<String, String> generatedSQL = databaseService.getGeneratedSQL();
        File generatedZipFile = databaseService.getGeneratedZipFile();
        assertTrue(generatedSQL.isEmpty());
        assertFalse(generatedZipFile.exists());
    }


    @Test
    void givenPropertyFile_whenGenerateSQL_thenDoNotSendEmail() {
        DatabaseService databaseService = new DatabaseService("application-no-email.properties");
        databaseService.generateSQLFromKeySpace();
        Map<String, String> generatedSQL = databaseService.getGeneratedSQL();
        File generatedZipFile = databaseService.getGeneratedZipFile();
        assertNotNull(generatedSQL);
        assertNotNull(generatedZipFile);
    }

}
