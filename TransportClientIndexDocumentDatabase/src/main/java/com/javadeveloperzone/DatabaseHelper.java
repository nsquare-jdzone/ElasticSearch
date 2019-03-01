package com.javadeveloperzone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseHelper {

    Connection connection;

    public Connection getConnection() {
        return connection;
    }

    /**
     * CREATE TABLE `document` (
     *   `docId` int(11) NOT NULL,
     *   `docType` varchar(255) DEFAULT NULL,
     *   `docTitle` varchar(255) DEFAULT NULL,
     *   `docAuthor` varchar(255) DEFAULT NULL,
     *   `docLanguage` varchar(45) DEFAULT NULL,
     *   `numberOfPage` int(11) DEFAULT NULL,
     *   `lastIndexDate` datetime DEFAULT NULL,
     *   PRIMARY KEY (`docId`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
     * */

    public void openMySqlDbConnection() throws ClassNotFoundException, SQLException {
//        Class.forName("com.mysql.jdbc.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/bulkindexing","root","jdzone");

    }

    public void updateRecords() throws SQLException {
        String query = "update document set lastIndexDate=now() where lastIndexDate>'2019-02-01 00:00:00'";
        Statement statement = connection.createStatement();
        int recordsUpdated = statement.executeUpdate(query);
        System.out.println("Record Updated : "+recordsUpdated);
    }
    public void closeMySqlDbConnection() {
        try{
            if(connection!=null){
                connection.close();
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
