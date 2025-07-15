package tcl;

import org.junit.jupiter.api.Test;
import sql.JDBCUtils;

import java.sql.*;

public class TCL {
    JDBCUtils jdbcUtils = new JDBCUtils();

    private void printAccountBalance(Connection connection, String tableName, String accountNumber) throws SQLException {
        String query = "SELECT balance FROM " + tableName + " WHERE account_number = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, accountNumber);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    System.out.println("Balance for account " + accountNumber + " in " + tableName + ": $" + rs.getBigDecimal("balance"));
                }
            }
        }
    }

    @Test
    void tclTest() throws SQLException {
    jdbcUtils.setUpDatabase(
        connection -> {
          // Define table creation for ACCOUNT A
          String query1 =
              "CREATE TABLE IF NOT EXISTS ACCOUNT_A ("
                  + "account_id INT PRIMARY KEY AUTO_INCREMENT, "
                  + "account_number VARCHAR(20) NOT NULL UNIQUE, "
                  + "account_holder VARCHAR(255) NOT NULL, "
                  + "balance DECIMAL(15,2) NOT NULL DEFAULT 0.00, "
                  + "created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                  + "status VARCHAR(10) DEFAULT 'ACTIVE')";
          try (Statement statement1 = connection.createStatement()) {
            statement1.execute(query1);
            System.out.println("Account_A created successfully!");
            // Deposit 1000.00 into ACCOUNT A
            String insertSQL =
                "INSERT INTO ACCOUNT_A (account_number, account_holder, balance) VALUES (?, ?, ?)";
            try (java.sql.PreparedStatement insertStatement =
                connection.prepareStatement(insertSQL)) {
              insertStatement.setString(1, "ACC-1000");
              insertStatement.setString(2, "John Doe");
              insertStatement.setBigDecimal(3, new java.math.BigDecimal("1000.00"));
              insertStatement.executeUpdate();
              System.out.println("Data inserted successfully!");
            }
            // Define table creation for ACCOUNT B
            String query2 =
                "CREATE TABLE IF NOT EXISTS ACCOUNT_B ("
                    + "account_id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "account_number VARCHAR(20) NOT NULL UNIQUE, "
                    + "account_holder VARCHAR(255) NOT NULL, "
                    + "balance DECIMAL(15,2) NOT NULL DEFAULT 0.00, "
                    + "created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                    + "status VARCHAR(10) DEFAULT 'ACTIVE')";
            try (Statement statement2 = connection.createStatement()) {
              statement2.execute(query2);
              System.out.println("Account_B created successfully!");
              // Deposit 1000.00 into ACCOUNT B
              String insertSQL2 =
                  "INSERT INTO ACCOUNT_B (account_number, account_holder, balance) VALUES (?, ?, ?)";
              try (java.sql.PreparedStatement insertStatement2 =
                  connection.prepareStatement(insertSQL2)) {
                insertStatement2.setString(1, "ACC-500");
                insertStatement2.setString(2, "Jane Doe");
                insertStatement2.setBigDecimal(3, new java.math.BigDecimal("1000.00"));
                insertStatement2.executeUpdate();
                System.out.println("Data inserted successfully!");
                printAccountBalance(connection, "ACCOUNT_A", "ACC-1000");
                printAccountBalance(connection, "ACCOUNT_B", "ACC-500");
              }
            }
            // Transaction for money transfer
            connection.setAutoCommit(false);
            try {
              // Deduct money from Account A
              String deductSQL =
                  "UPDATE \"ACCOUNT_A\" SET balance = balance - ? WHERE account_number = ?";
              try (PreparedStatement deductStmt = connection.prepareStatement(deductSQL)) {
                deductStmt.setBigDecimal(1, new java.math.BigDecimal("100.00"));
                deductStmt.setString(2, "ACC-1000");
                deductStmt.executeUpdate();
              }

              // Add money to Account B
              String addSQL =
                  "UPDATE \"ACCOUNT_B\" SET balance = balance + ? WHERE account_number = ?";
              try (PreparedStatement addStmt = connection.prepareStatement(addSQL)) {
                addStmt.setBigDecimal(1, new java.math.BigDecimal("100.00"));
                addStmt.setString(2, "ACC-500");
                addStmt.executeUpdate();
              }
              connection.commit();
              System.out.println("Transaction completed successfully!");
              printAccountBalance(connection, "ACCOUNT_A", "ACC-1000");
              printAccountBalance(connection, "ACCOUNT_B", "ACC-500");

            } catch (SQLException e) {
              connection.rollback();
              System.err.println("Transaction failed, rolling back: " + e.getMessage());
            } finally {
              connection.setAutoCommit(true);
            }

          } catch (SQLException e) {
            System.err.println("Table creation failed: " + e.getMessage());
          }
        });
    }
}
