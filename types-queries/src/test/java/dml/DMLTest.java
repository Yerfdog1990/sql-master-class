package dml;

import ddl.DDLTest;
import org.junit.jupiter.api.Test;
import sql.JDBCUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DMLTest {
    JDBCUtils jdbcUtils = new JDBCUtils();

    @Test
    void dmlTest() throws SQLException {
    jdbcUtils.setUpDatabase(
        connection -> {

          // Create a SCHOOL table
          String createSQL =
              "CREATE TABLE IF NOT EXISTS SCHOOL (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))";
          try (PreparedStatement createStatement = connection.prepareStatement(createSQL)) {
            createStatement.executeUpdate();
            System.out.println("Table created successfully!");
          } catch (SQLException e) {
            System.err.println("Create failed: " + e.getMessage());
          }

          // Insert data into the SCHOOL table
          String insertSQL = "INSERT INTO SCHOOL (name) VALUES (?)";
          try (PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "University of the People, USA");
            insertStatement.addBatch();
            insertStatement.setString(1, "University of California, USA");
            insertStatement.addBatch();
            insertStatement.setString(1, "Kampala University, Uganda");
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Tokyo, Japan");
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Hawaii, USA");
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Michigan, USA");
            insertStatement.addBatch();
            insertStatement.executeBatch();
            System.out.println("Data inserted successfully!");

            // 3. Update SCHOOL table
              String updateSQL = "UPDATE SCHOOL SET name = ? WHERE id = ?";
              try (PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
                updateStatement.setString(1, "University of Miami, USA");
                updateStatement.setInt(2, 1);
                updateStatement.executeUpdate();
                System.out.println("Data updated successfully!");

                // 4. Delete a row
                  String deleteSQL = "DELETE FROM SCHOOL WHERE id = ?";
                  try (PreparedStatement deleteStatement = connection.prepareStatement(deleteSQL)) {
                    deleteStatement.setInt(1, 2);
                    deleteStatement.executeUpdate();
                    System.out.println("Data deleted successfully!");
                  }
              }
          } catch (SQLException e) {
            System.err.println("Insert failed: " + e.getMessage());
          }
        });
    }
}
