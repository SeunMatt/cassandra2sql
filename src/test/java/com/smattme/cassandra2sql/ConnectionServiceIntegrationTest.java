package com.smattme.cassandra2sql;


import com.smattme.cassandra2sql.config.Constants;
import com.smattme.cassandra2sql.services.ConnectionService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConnectionServiceIntegrationTest {

    private static ConnectionService connectionService;
    private static Logger logger = LoggerFactory.getLogger(ConnectionServiceIntegrationTest.class);

    @BeforeAll
    static void init() {
        Properties properties = new Properties();
        properties.setProperty(Constants.DB_HOST, "127.0.0.1");
        properties.setProperty(Constants.DB_PORT, "9042");
        properties.setProperty(Constants.DB_PASSWORD, "");
        properties.setProperty(Constants.DB_USERNAME, "");
        connectionService = ConnectionService.getInstance(properties);
    }

    @AfterAll
    static void closeConnection() {
        connectionService.closeConnection();
    }

    @Test
    void whenGetSession_thenReturnDBSession() {
        assertNotNull(connectionService.getSession());
    }


}
