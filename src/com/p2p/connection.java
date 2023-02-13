package com.p2p;// Java Program to Retrieve Contents of a Table Using JDBC
// connection

// Showing linking of created database

// Importing SQL libraries to create database

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class connection {

    Connection con = null;

    public static Connection connectDB()

    {

        try {
            // Importing and registering drivers
            Class.forName("com.mysql.jdbc.Driver");

            Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/p2p",
                    "root", "password");

            return con;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
