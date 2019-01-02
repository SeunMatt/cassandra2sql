package com.smattme.cassandra2sql.services;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.smattme.cassandra2sql.config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ConnectionService {

    private Cluster cluster;
    private Session session;
    private static ConnectionService INSTANCE;
    private static Logger logger = LoggerFactory.getLogger(ConnectionService.class);
    private String DB_HOST, DB_USERNAME, DB_PASSWORD;
    private int DB_PORT;

    private ConnectionService(String DB_HOST, int DB_PORT, String DB_PASSWORD, String DB_USERNAME) {
        this.DB_HOST = DB_HOST;
        this.DB_PASSWORD = DB_PASSWORD;
        this.DB_PORT = DB_PORT;
        this.DB_USERNAME = DB_USERNAME;
    }

    public static ConnectionService getInstance(Properties properties) {
        if(Objects.isNull(INSTANCE)) {
            logger.info("creating a new instance of " + ConnectionService.class.getName());
            INSTANCE = new ConnectionService(
                    properties.getProperty(Constants.DB_HOST, "127.0.0.1"),
                    Integer.parseInt(properties.getProperty(Constants.DB_PORT, "9042")),
                    properties.getProperty(Constants.DB_PASSWORD, ""),
                    properties.getProperty(Constants.DB_USERNAME, "")
            );
        }

        return INSTANCE;
    }


    private Cluster getCluster() {

        if(Objects.isNull(cluster) || cluster.isClosed()) {
            logger.info("initializing database cluster connection");
            cluster = Cluster.builder()
                    .addContactPoint(DB_HOST)
                    .withPort(DB_PORT)
                    .withCredentials(DB_USERNAME, DB_PASSWORD)
                    .build();
        }

        return cluster;

    }

    public Session getSession() {
        if(Objects.isNull(session) || session.isClosed()) {
            logger.info("initializing database session connection");
            session = getCluster().connect();
        }

        return session;
    }


    public void closeConnection() {
        if (!Objects.isNull( cluster)) {
            logger.info("closing database connection");
            cluster.close();
        }
    }

}
