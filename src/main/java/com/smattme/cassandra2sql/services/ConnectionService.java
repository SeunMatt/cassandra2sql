package com.smattme.cassandra2sql.services;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ConnectionService {

    private Cluster cluster;
    private Session session;
    private static ConnectionService INSTANCE;
    private static Logger logger = LoggerFactory.getLogger(ConnectionService.class);

    private ConnectionService() {}

    public static ConnectionService getInstance() {
        if(Objects.isNull(INSTANCE)) {
            logger.info("creating a new instance of " + ConnectionService.class.getName());
            INSTANCE = new ConnectionService();
        }

        return INSTANCE;
    }


    private Cluster getCluster() {

        if(Objects.isNull(cluster) || cluster.isClosed()) {
            logger.info("initializing database cluster connection");
            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
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
