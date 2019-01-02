package com.smattme.cassandra2sql.config;

public enum SQLFlavour {

    NO_FLAVOUR("NILL"),
    MYSQL_FLAVOUR("MYSQL"),
    POSTGRES_FLAVOUR("POSTGRES");

    private String value;

    SQLFlavour(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
