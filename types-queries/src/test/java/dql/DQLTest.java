package dql;

import org.junit.jupiter.api.Test;
import sql.JDBCUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DQLTest {
    JDBCUtils jdbcUtils = new JDBCUtils();

    @Test
    void dqlTest() throws SQLException {
    jdbcUtils.setUpDatabase(
        connection -> {
          try {
            setUPDatabase();
            // 1. Retrieve ALL column
            System.out.println("\nRetrieve ALL column:");
            String selectSQL = "SELECT * FROM SCHOOL";
            try (PreparedStatement retrieveAllColumns = connection.prepareStatement(selectSQL)) {
              try (ResultSet resultSet = retrieveAllColumns.executeQuery()) {
                while (resultSet.next()) {
                  int id = resultSet.getInt("id");
                  String name = resultSet.getString("name");
                  String address = resultSet.getString("address");
                  int population = resultSet.getInt("population");
                  int rank = resultSet.getInt("rank");
                  System.out.println(
                      "ID: "
                          + id
                          + ", Name: "
                          + name
                          + ", Address: "
                          + address
                          + ", Population: "
                          + population
                          + ", Rank: "
                          + rank);
                }
              }
            }

            // 2. Retrieve specific columns
            System.out.println("\nRetrieve specific columns:");
            String selectSQL2 = "SELECT name, population FROM SCHOOL";
            try (PreparedStatement retrieveSpecificColumns =
                connection.prepareStatement(selectSQL2)) {
              try (ResultSet resultSet = retrieveSpecificColumns.executeQuery()) {
                while (resultSet.next()) {
                  String name = resultSet.getString("name");
                  int population = resultSet.getInt("population");
                  System.out.println("Name: " + name + ", Population: " + population);
                }
              }
            }

            // 3. Retrieve rows that meet specific conditions:
            System.out.println("\nRetrieve rows that meet specific conditions:");
            String conditionallyRetrieveRows = "SELECT * FROM SCHOOL WHERE population > 35000";
            try (PreparedStatement conditionallyRetrieveRowsStatement =
                connection.prepareStatement(conditionallyRetrieveRows)) {
              try (ResultSet resultSet = conditionallyRetrieveRowsStatement.executeQuery()) {
                while (resultSet.next()) {
                  int id = resultSet.getInt("id");
                  String name = resultSet.getString("name");
                  String address = resultSet.getString("address");
                  int population = resultSet.getInt("population");
                  int rank = resultSet.getInt("rank");
                  System.out.println(
                      "ID: "
                          + id
                          + ", Name: "
                          + name
                          + ", Address: "
                          + address
                          + ", Population: "
                          + population
                          + ", Rank: "
                          + rank);
                }
              }
            }

            // 4. Aggregate Functions
            System.out.println("\nAggregate Functions:");
            String aggregateSQL =
                "SELECT COUNT(*) as count, AVG(population) as avg_pop, MAX(population) as max_pop FROM SCHOOL";
            try (PreparedStatement aggregateStatement = connection.prepareStatement(aggregateSQL)) {
              try (ResultSet resultSet = aggregateStatement.executeQuery()) {
                while (resultSet.next()) {
                  int count = resultSet.getInt("count");
                  double avgPop = resultSet.getDouble("avg_pop");
                  int maxPop = resultSet.getInt("max_pop");
                  System.out.println(
                      "Total Schools: "
                          + count
                          + ", Average Population: "
                          + avgPop
                          + ", Maximum Population: "
                          + maxPop);
                }
              }
            }

            // 5. GROUP BY
            System.out.println("\nGROUP BY Example:");
            String groupBySQL =
                "SELECT SUBSTRING(address, POSITION(',' IN address) + 2) as country, COUNT(*) as school_count "
                    + "FROM SCHOOL GROUP BY SUBSTRING(address, POSITION(',' IN address) + 2)";
            try (PreparedStatement groupByStatement = connection.prepareStatement(groupBySQL)) {
              try (ResultSet resultSet = groupByStatement.executeQuery()) {
                while (resultSet.next()) {
                  String country = resultSet.getString("country");
                  int count = resultSet.getInt("school_count");
                  System.out.println("Country: " + country + ", Number of Schools: " + count);
                }
              }
            }

            // 6. ORDER BY
            System.out.println("\nORDER BY Example:");
            String orderBySQL = "SELECT name, population FROM SCHOOL ORDER BY population DESC";
            try (PreparedStatement orderByStatement = connection.prepareStatement(orderBySQL)) {
              try (ResultSet resultSet = orderByStatement.executeQuery()) {
                while (resultSet.next()) {
                  String name = resultSet.getString("name");
                  int population = resultSet.getInt("population");
                  System.out.println("School: " + name + ", Population: " + population);
                }
              }
            }

            // 7. DISTINCT
            System.out.println("\nDISTINCT Example:");
            String distinctSQL =
                "SELECT DISTINCT SUBSTRING(address, POSITION(',' IN address) + 2) as country FROM SCHOOL";
            try (PreparedStatement distinctStatement = connection.prepareStatement(distinctSQL)) {
              try (ResultSet resultSet = distinctStatement.executeQuery()) {
                while (resultSet.next()) {
                  String country = resultSet.getString("country");
                  System.out.println("Country: " + country);
                }
              }
            }

          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
    }
    private void setUPDatabase() throws SQLException {
        jdbcUtils.setUpDatabase(connection -> {
            // Create a table
            
            String createSQL = "CREATE TABLE IF NOT EXISTS SCHOOL (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), address VARCHAR(255), population INT, rank INT)";
            try(PreparedStatement createStatement = connection.prepareStatement(createSQL)){
                createStatement.executeUpdate();
                System.out.println("Table created successfully!");
            } catch (SQLException e) {
                System.err.println("Table creation failed: " + e.getMessage());
            }
        });
    jdbcUtils.setUpDatabase(
        connection -> {

          // Insert data into the table
          String insertSQL =
              "INSERT INTO SCHOOL (name, address, population, rank) VALUES (?, ?, ?, ?)";
          try (PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "University of the People, USA");
            insertStatement.setString(2, "California, USA");
            insertStatement.setInt(3, 5000);
            insertStatement.setInt(4, 1);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of California, USA");
            insertStatement.setString(2, "Berkeley, USA");
            insertStatement.setInt(3, 42000);
            insertStatement.setInt(4, 2);
            insertStatement.addBatch();
            insertStatement.setString(1, "Kampala University, Uganda");
            insertStatement.setString(2, "Kampala, Uganda");
            insertStatement.setInt(3, 15000);
            insertStatement.setInt(4, 3);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Tokyo, Japan");
            insertStatement.setString(2, "Tokyo, Japan");
            insertStatement.setInt(3, 28000);
            insertStatement.setInt(4, 4);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Hawaii, USA");
            insertStatement.setString(2, "Hawaii, USA");
            insertStatement.setInt(3, 18000);
            insertStatement.setInt(4, 5);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Michigan, USA");
            insertStatement.setString(2, "Michigan, USA");
            insertStatement.setInt(3, 45000);
            insertStatement.setInt(4, 6);
            insertStatement.addBatch();
            insertStatement.setString(1, "Oxford University, UK");
            insertStatement.setString(2, "Oxford, UK");
            insertStatement.setInt(3, 24000);
            insertStatement.setInt(4, 7);
            insertStatement.addBatch();
            insertStatement.setString(1, "Cambridge University, UK");
            insertStatement.setString(2, "Cambridge, UK");
            insertStatement.setInt(3, 23000);
            insertStatement.setInt(4, 8);
            insertStatement.addBatch();
            insertStatement.setString(1, "ETH Zurich, Switzerland");
            insertStatement.setString(2, "Zurich, Switzerland");
            insertStatement.setInt(3, 21000);
            insertStatement.setInt(4, 9);
            insertStatement.addBatch();
            insertStatement.setString(1, "National University of Singapore");
            insertStatement.setString(2, "Singapore");
            insertStatement.setInt(3, 38000);
            insertStatement.setInt(4, 10);
            insertStatement.addBatch();
            insertStatement.setString(1, "Peking University, China");
            insertStatement.setString(2, "Beijing, China");
            insertStatement.setInt(3, 46000);
            insertStatement.setInt(4, 11);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Melbourne, Australia");
            insertStatement.setString(2, "Melbourne, Australia");
            insertStatement.setInt(3, 52000);
            insertStatement.setInt(4, 12);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Toronto, Canada");
            insertStatement.setString(2, "Toronto, Canada");
            insertStatement.setInt(3, 93000);
            insertStatement.setInt(4, 13);
            insertStatement.addBatch();
            insertStatement.setString(1, "Technical University of Munich, Germany");
            insertStatement.setString(2, "Munich, Germany");
            insertStatement.setInt(3, 43000);
            insertStatement.setInt(4, 14);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Cape Town, South Africa");
            insertStatement.setString(2, "Cape Town, South Africa");
            insertStatement.setInt(3, 29000);
            insertStatement.setInt(4, 15);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of São Paulo, Brazil");
            insertStatement.setString(2, "São Paulo, Brazil");
            insertStatement.setInt(3, 88000);
            insertStatement.setInt(4, 16);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Amsterdam, Netherlands");
            insertStatement.setString(2, "Amsterdam, Netherlands");
            insertStatement.setInt(3, 34000);
            insertStatement.setInt(4, 17);
            insertStatement.addBatch();
            insertStatement.setString(1, "Seoul National University, South Korea");
            insertStatement.setString(2, "Seoul, South Korea");
            insertStatement.setInt(3, 28000);
            insertStatement.setInt(4, 18);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Vienna, Austria");
            insertStatement.setString(2, "Vienna, Austria");
            insertStatement.setInt(3, 94000);
            insertStatement.setInt(4, 19);
            insertStatement.addBatch();
            insertStatement.setString(1, "University of Oslo, Norway");
            insertStatement.setString(2, "Oslo, Norway");
            insertStatement.setInt(3, 28000);
            insertStatement.setInt(4, 20);
            insertStatement.addBatch();
            insertStatement.executeBatch();
            System.out.println("Data inserted successfully!");
          } catch (SQLException e) {
            System.err.println("Insert failed: " + e.getMessage());
          }
        });
    }
}
