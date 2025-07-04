package sql;

import java.sql.*;
import java.util.function.Consumer;

public class JDBCUtils {
    public void setUpDatabase(Consumer<Connection> consumer) throws SQLException {
        String userName = "sa";
        String password = "";
        String url = "jdbc:h2:mem:school;DB_CLOSE_DELAY=-1";
        try (Connection connection = DriverManager.getConnection(url, userName, password)) {
            System.out.println("Database connected successfully!");

            // Creating the schema and inserting seed data
            setupSchemaAndData(connection);

            // Pass the connection for further use
            consumer.accept(connection);
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            throw e; // Ensure errors propagate for handling
        }
        System.out.println("Connection closed");
    }

    private void setupSchemaAndData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // Create a sample table (if not exists) and seed data
            statement.execute("CREATE TABLE IF NOT EXISTS students (id INT PRIMARY KEY, name VARCHAR(100), grade INT)");
            statement.execute("MERGE INTO students KEY(id) VALUES (1, 'Alice', 3), (2, 'Bob', 4)"); // Seed data
            System.out.println("Schema and data setup complete!");
        }
    }
}