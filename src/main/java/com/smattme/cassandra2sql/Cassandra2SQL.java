package com.smattme.cassandra2sql;

import com.smattme.cassandra2sql.services.DatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassandra2SQL {

    private static Logger logger = LoggerFactory.getLogger(Cassandra2SQL.class);

    public static void main(String ... args) {
        logger.info("Supplied properties file path: " + args[0]);
        DatabaseService databaseService = new DatabaseService(args[0]);
        databaseService.generateSQLFromKeySpace();
    }

}
