package ddl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sql.JDBCUtils;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DDLTest {
    JDBCUtils jdbcUtils = new JDBCUtils();

    @Test
    void ddlTest() throws SQLException {
        jdbcUtils.setUpDatabase(
                connection -> {
                    try {
                        // Define table creation SQL commands
                        String table1 = "CREATE TABLE IF NOT EXISTS school (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))";
                        String table2 = "CREATE TABLE IF NOT EXISTS student (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), school_id INT, FOREIGN KEY (school_id) REFERENCES school(id))";
                        String table3 = "CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), student_id INT, FOREIGN KEY (student_id) REFERENCES student(id))";
                        String table4 = "CREATE TABLE IF NOT EXISTS teacher (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))";
                        // Create tables
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(table1);
                            System.out.println("Table 1 created successfully!");
                            statement.execute(table2);
                            System.out.println("Table 2 created successfully!");
                            statement.execute(table3);
                            System.out.println("Table 3 created successfully!");
                            statement.execute(table4);
                            System.out.println("Table 4 created successfully!");
                        }

                        // Verify the existence of the SCHOOL table
                        checkTableExists(connection, "SCHOOL");

                        // Verify the structure of the STUDENT table
                        checkTableStructure(connection, "STUDENT", 3);

                        // Verify the existence of the PARENT table
                        checkTableExists(connection, "PARENT");

                        // ALTER table
                        try(Statement statement = connection.createStatement()){
                            // 1. Add a column
                            statement.execute("ALTER TABLE SCHOOL ADD COLUMN address VARCHAR(255)");
                            // Verify the structure of the SCHOOL table
                            checkTableStructure(connection, "SCHOOL", 3);

                            // 2. Modify column data type
                            statement.execute("ALTER TABLE SCHOOL ALTER COLUMN address INT");
                            checkDataType(connection);

                            // 3. Drop a column
                            statement.execute("ALTER TABLE SCHOOL DROP COLUMN address");
                            checkTableStructure(connection, "SCHOOL", 2);
                        }

                        // DROP table
                        try(Statement statement = connection.createStatement()){
                            statement.execute("DROP TABLE TEACHER");
                            // Check the non-existence of the SCHOOL table
                            checkTableExists(connection, "TEACHER");
                        }
                    } catch (SQLException e) {
                        System.err.println("Table creation failed: " + e.getMessage());
                    }
                }
        );
    }

    // Helper method to check if a table exists
    private void checkTableExists(Connection connection, String tableName) throws SQLException {
        ResultSet tableInfo = connection.getMetaData().getTables(null, null, tableName.toUpperCase(), null);
        if (tableInfo.next()) {
            System.out.println(tableName + " table exists.");
        } else {
            System.err.println(tableName + " table does not exist.");
        }
    }

    // Helper method to check the structure of a table
    private void checkTableStructure(Connection connection, String tableName, int expectedColumnCount) throws SQLException {
        String query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?";

        // Use try-with-resources to auto-close resources
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            // Bind the table name to the query parameter
            preparedStatement.setString(1, tableName.toUpperCase());

            // Execute the query
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    // Retrieve the count from the ResultSet
                    int actualColumnCount = resultSet.getInt(1);
                    System.out.println(tableName + " table has " + actualColumnCount + " columns.");

                    // Assert the column count
                    assertEquals(expectedColumnCount, actualColumnCount);
                }
            }
        }
    }

    // Helper method to check data type
    private void checkDataType(Connection connection) throws SQLException{
        String query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";

        // Use try-with-resources to auto-close resources
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)){
            preparedStatement.setString(1, "SCHOOL".toUpperCase());
            preparedStatement.setString(2, "address".toUpperCase());

            // Execute the query
            try(ResultSet resultSet = preparedStatement.executeQuery()){
                if(resultSet.next()) {
                    String actualDataType = resultSet.getString(1);
                    System.out.println("SCHOOL" + " table column " + "address" + " has " + actualDataType + " data type.");
                    Assertions.assertEquals("INTEGER", actualDataType);
                }
            }
        }
    }
}