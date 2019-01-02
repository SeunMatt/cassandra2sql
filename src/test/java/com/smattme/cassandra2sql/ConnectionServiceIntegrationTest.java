package com.smattme.cassandra2sql;


import com.smattme.cassandra2sql.services.ConnectionService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConnectionServiceIntegrationTest {

    private static ConnectionService connectionService;
    private static Logger logger = LoggerFactory.getLogger(ConnectionServiceIntegrationTest.class);

    @BeforeAll
    static void init() {
        connectionService = ConnectionService.getInstance();
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
