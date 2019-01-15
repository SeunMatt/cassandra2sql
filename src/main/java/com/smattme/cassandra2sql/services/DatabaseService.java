package com.smattme.cassandra2sql.services;

import com.datastax.driver.core.*;
import com.datastax.driver.core.DataType.Name;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smattme.cassandra2sql.config.Constants;
import com.smattme.cassandra2sql.config.SQLFlavour;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.zip.ZipUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.smattme.cassandra2sql.config.Constants.*;

public class DatabaseService {

    private Session databaseSession;
    private ConnectionService conectionService;
    private String keySpace;
    private Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private String identifierQuote;
    private String NULL_VALUE;
    private String dateTimeFormat, dateFormat;
    private String tempDirName = "cassandra2sql";
    private File tempDir, generatedZipFile;
    private Properties properties;
    private Map<String, String> generatedSQL;
    private boolean keepGeneratedSQLFile;
    private String configValueSeparator = ":";

    public DatabaseService(String pathToProperties) {
        init(pathToProperties);
    }

    /**
     * BOOTSTRAP THE APP WITH CONFIGS
     * @param pathToProperties a complete path to the application.properties file
     */
    private void init(String pathToProperties) {

        //load config file
        properties = new Properties();
        try {
            properties.load(new BufferedInputStream(new FileInputStream(pathToProperties)));
        } catch (IOException e) {
            logger.error("Exception while reading the config file: " + e.getLocalizedMessage(), e);
            System.exit(0);
        }

        conectionService = ConnectionService.getInstance(properties);
        databaseSession = conectionService.getSession();
        generatedSQL = new HashMap<>();
        NULL_VALUE = null;

        keepGeneratedSQLFile = Boolean.parseBoolean(properties.getProperty(KEEP_GENERATED_SQL, "false"));

        keySpace = properties.getProperty(KEYSPACE);
        if(StringUtils.isEmpty(keySpace)) {
            logger.error("No keyspace/database is specified in the config file");
            System.exit(0);
        }

        dateTimeFormat = properties.getProperty(DATETIME_FORMAT, "yyyy-MM-dd HH:mm:ss");
        dateFormat = properties.getProperty(DATE_FORMAT, "yyyy-MM-dd");

        SQLFlavour sqlFlavour = StringUtils.equals(properties.getProperty(SQL_FLAVOUR, SQLFlavour.POSTGRES_FLAVOUR.getValue()), SQLFlavour.POSTGRES_FLAVOUR.getValue()) ?
                SQLFlavour.POSTGRES_FLAVOUR : SQLFlavour.MYSQL_FLAVOUR;
        if(sqlFlavour.equals(SQLFlavour.POSTGRES_FLAVOUR)) {
            identifierQuote = "\"";
        }
        else if(sqlFlavour.equals(SQLFlavour.MYSQL_FLAVOUR)) {
            identifierQuote = "`";
        }
        else {
            identifierQuote = ""; //the default
        }



        //init temp dir for storing generated file
        //create a temp dir to store the exported file for processing
        try {

            tempDir = new File(tempDirName);

            if(tempDir.exists())
                //attempt to delete folder if it exists already
                Files.walk(Paths.get(tempDirName)).map(Path::toFile).forEach(File::delete);

            //then recreate it
            if(!tempDir.exists()) {
                boolean res = tempDir.mkdir();
                if(!res) {
                    logger.error("Unable to create temp dir: " + tempDir.getAbsolutePath());
                }
            }

        } catch (Exception e) {
            logger.warn("unable to delete existing temp folder: [" + tempDir + "]. This means the final zip file might contain other files");
            logger.warn("Error: " + e.getLocalizedMessage(), e);
        }

    }


    private String generateSQLFromTable(String srcTableName, String targetTableName, String columnMap) {

        ResultSet rows = databaseSession.execute("SELECT * FROM " + keySpace + "." + srcTableName + ";");

        int total = rows.getAvailableWithoutFetching();
        logger.debug("table " + srcTableName + " has at least [" + total + "] rows");
        if(total <= 0) {
            return "";
        }

        //EXTRACT the srcColumns
        List<String> srcColumns = new LinkedList<>();

        //merging to values
        String targetColumns = Arrays.stream(columnMap.split(","))
                .map(s ->  {
                    String [] arr = s.split(configValueSeparator);
                    //save the order of src column for processing later on
                    srcColumns.add(arr[0]);
                    return identifierQuote + arr[1] + identifierQuote;
                })
                .collect(Collectors.joining(", ")).trim();

        final String[] insertStatement = {"INSERT INTO " + identifierQuote +  targetTableName + identifierQuote + " ( " + targetColumns + ") VALUES "};

        //process the rows.
        //we can do parallel processing of the rows but
        //the columns of each row has to be processed orderly
        rows.all().parallelStream().forEach(row -> {
            //process each columns of the rows
            //in the order they're stored in the srcColumns list
            String values = srcColumns.stream()
                    .map(column -> extractColumnValue(row, column))
                    .collect(Collectors.joining(","));

            //add the insert string for a row to the whole
            insertStatement[0] += "(" + values + "), ";

        });

        String insertStatementFinal = insertStatement[0].trim();
        insertStatementFinal = insertStatementFinal.substring(0, insertStatementFinal.lastIndexOf(","));

//        logger.debug("insert statement generated : \n" + insertStatementFinal);

        return insertStatementFinal;
    }

    private void saveSQLToFile(String sql, String tableName) {
        String sqlFileName = tableName.toLowerCase().concat("_").concat(LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss"))) + ".sql";
        try {
            FileOutputStream outputStream = new FileOutputStream( tempDirName + "/" + sqlFileName);
            outputStream.write(sql.getBytes());
            outputStream.close();
        } catch (Exception e) {
            logger.error("Error Saving generated SQL to file: " + e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    /**
     * coordinate other functions to
     * perform the export
     */
    private void generateSQLFromKeySpace() {

        String tables = properties.getProperty(Constants.TABLES_KEY);
        logger.info("properties table List: \n" + tables);
        List<String> tablesList = Arrays.asList(tables.split(","));

        //validate column mappings for the

        tablesList.parallelStream().forEach(table -> {

            String [] tableArray = table.split(configValueSeparator);
            String sourceTable, targetTable;

            if(tableArray.length == 1) {
                sourceTable = targetTable = tableArray[0];
            }
            else {
                sourceTable = tableArray[0];
                targetTable = tableArray[1];
            }

            logger.debug("processing source table: " + sourceTable + " and TargetTable [" + targetTable + "]");

            String columnMap = properties.getProperty(Constants.TABLE_CONFIG_PREFIX + sourceTable.toUpperCase());
            logger.debug("columnMap: \n" + columnMap);

            if(Objects.isNull(columnMap)) {
                logger.error("table " + sourceTable + " does not have a column mapping config");
            }
            else {
                String generatedInsertStatement = generateSQLFromTable(sourceTable, targetTable, columnMap);
                if(!StringUtils.isEmpty(generatedInsertStatement)) {
                    if (keepGeneratedSQLFile)
                        generatedSQL.put(targetTable, generatedInsertStatement);

                    saveSQLToFile(generatedInsertStatement, targetTable);
                }
            }
        });

        //once we're done, generate a zip file
        String [] filenameCheck = tempDir.list();

        //check if dir is empty or not
        if(!Objects.isNull(filenameCheck) && filenameCheck.length > 0) {

            String generatedZipFileName = keySpace.toLowerCase().concat("_").concat(LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss"))) + ".zip";
            this.generatedZipFile = new File(generatedZipFileName);
            ZipUtil.pack(tempDir, generatedZipFile);

            //mail the zipped file if mail settings are available
            if (isEmailPropertiesSet()) {
                boolean emailSendingRes = EmailService.builder()
                        .setHost(properties.getProperty(EMAIL_HOST))
                        .setPort(Integer.valueOf(properties.getProperty(EMAIL_PORT)))
                        .setToAddress(properties.getProperty(EMAIL_TO))
                        .setFromAddress(properties.getProperty(EMAIL_FROM))
                        .setUsername(properties.getProperty(EMAIL_USERNAME))
                        .setPassword(properties.getProperty(EMAIL_PASSWORD))
                        .setSubject(properties.getProperty(EMAIL_SUBJECT, generatedZipFileName))
                        .setMessage(properties.getProperty(EMAIL_MESSAGE, "Please find attached backup of key-space " + keySpace))
                        .setAttachments(new File[]{generatedZipFile})
                        .sendMail();

                if (emailSendingRes) {
                    logger.debug("Zip File Sent as Attachment to Email Address Successfully");
                } else {
                    logger.error("Unable to send zipped file as attachment to email. See log debug for more info");
                }
            }

        }

    }


    /**
     * This function will check if all the minimum
     * required email properties are set,
     * that can facilitate sending of exported
     * sql to email
     * @return bool
     */
    private boolean isEmailPropertiesSet() {
        return properties != null &&
                properties.containsKey(EMAIL_HOST) &&
                properties.containsKey(EMAIL_PORT) &&
                properties.containsKey(EMAIL_USERNAME) &&
                properties.containsKey(EMAIL_PASSWORD) &&
                properties.containsKey(EMAIL_FROM) &&
                properties.containsKey(EMAIL_TO);
    }

    /**
     * this will extract and format the value of each
     * column in the row according to their type
     * @param row .
     * @param columnName .
     * @return the String value of the column in the row
     */
    private String extractColumnValue(Row row, String columnName) {

        if(Objects.isNull(row.getObject(columnName)))
            return NULL_VALUE;

        ColumnDefinitions definitions = row.getColumnDefinitions();
        String value;

        if(definitions.getType(columnName).isCollection()) {
            //convert it to a JSON string
            ObjectMapper objectMapper = new ObjectMapper();
            Object object = row.getObject(columnName);
            try {
                 value = "'" + objectMapper.writeValueAsString(object) + "'";
                 value = value.replace("'", "''");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                value = "'" + (!Objects.isNull(row.getObject(columnName)) ? row.getObject(columnName).toString() : NULL_VALUE).replace("'", "''") + "'";
            }
        }
        else if(definitions.getType(columnName).getName().equals(Name.TIMESTAMP)) {
            Date timestamp = row.getTimestamp(columnName);
            value = "'" + LocalDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(dateTimeFormat)) + "'";
        }
        else if(definitions.getType(columnName).getName().equals(Name.BOOLEAN)) {
            value = Boolean.toString(row.getBool(columnName));
        }
        else if(definitions.getType(columnName).getName().equals(Name.DATE)) {
            LocalDate date = row.getDate(columnName);
            value = "'" + java.time.LocalDate.of(date.getYear(), date.getMonth(), date.getDay()).format(DateTimeFormatter.ofPattern(dateFormat)) + "'";
        }
        else {
            try {
                value = "'" + row.getString(columnName).replace("'", "''") + "'";
            }
            catch (Exception e) {
                value = "'" + (!Objects.isNull(row.getObject(columnName)) ? row.getObject(columnName).toString() : NULL_VALUE).replace("'", "''") + "'";
            }
        }

        return  value;
    }


    /**
     * this will delete the temp folders and generated zip file
     * created
     */
    private void cleanTempFiles() {

        logger.debug("clearing temp files");

        //attempt to delete folder if it exists already
        try {

            if(!Objects.isNull(tempDir)) {
                logger.debug("deleting temp dir [" + tempDir.getAbsolutePath() + "]");
                Files.walk(tempDir.toPath()).map(Path::toFile).forEach(File::delete);
                tempDir.delete();
            }

            if(!Objects.isNull(generatedZipFile) && Boolean.parseBoolean(properties.getProperty(Constants.DELETE_GENERATED_ZIPFILE, "false"))) {
                logger.debug("deleting generated zip file [" + generatedZipFile.getAbsolutePath() + "]");
                Files.deleteIfExists(generatedZipFile.toPath());
            }

        } catch (IOException e) {
            logger.error("Unable to delete temp files generated: " + e.getLocalizedMessage(), e);
        }

    }


    public File getGeneratedZipFile() {
        return generatedZipFile;
    }

    public Map<String, String> getGeneratedSQL() {
        return generatedSQL;
    }

    /**
     * main application entry point
     * delegates to the generateSQLFromKeySpace()
     *  method. But handles failure and cleanups
     */
    public void performExport() {

        try {

            generateSQLFromKeySpace();

            logger.info("Process completed successfully");

        }  catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            //cleanup, we're done here
            cleanTempFiles();
            logger.debug("closing database connection");
            conectionService.closeConnection();
        }
    }

}
