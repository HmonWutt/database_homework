package org.example;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream("db.properties")){
            props.load(input);
            String url = props.getProperty("db.url");
            String user = props.getProperty("db.user");
            String password = props.getProperty("db.password");
            try (Connection connection = DriverManager.getConnection(url, user, password)) {
                System.out.println("Connected to furniture store database!");

                /********************fetching data (multiple rows from some table)*****************/
                PreparedStatement getAllRowsStatement = connection.prepareStatement(
                        """
                                SELECT * FROM customer
                            """);
                ResultSet rsGet = getAllRowsStatement.executeQuery();
                System.out.println("\n----------------fetching data (multiple rows from some table)----------------");
                while(rsGet.next()){
                    String firstName = rsGet.getString(5);
                    String lastName = rsGet.getString(6);
                    String city = rsGet.getString(4);
                    System.out.println(firstName+" "+lastName+" live in "+city+".");
                }
                rsGet.close();
                getAllRowsStatement.close();

                /***********************creating data in a table************************/
                PreparedStatement postStatement = connection.prepareStatement(
                        """
                                INSERT INTO customer (
                                address,birth_date, city, first_name, last_name,postal_code)
                                values (?,?,?,?,?,?)  
                            """,Statement.RETURN_GENERATED_KEYS );
                ArrayList<String> values = new ArrayList<>(List.of("Amiralsgatan 43", "1989-06-03","Malmö","Jane","Dough","21437"));
                for (int i = 1; i <= values.size(); i++){
                    postStatement.setString(i,values.get(i-1));
                }
                postStatement.executeUpdate();
                try( ResultSet generatedKeys = postStatement.getGeneratedKeys()){
                    if (generatedKeys.next()) {
                        int customerId = generatedKeys.getInt(1);
                        PreparedStatement getNewlyAddedCustomer = connection.prepareStatement(
                                """
                                    SELECT * FROM customer WHERE id = ?
                                """);
                        getNewlyAddedCustomer.setInt(1, customerId);
                        ResultSet rsNewlyAddedCustomer = getNewlyAddedCustomer.executeQuery();
                        System.out.println("\n----------------creating data in a tabl----------------");
                        ResultSetMetaData newCustomerData = rsNewlyAddedCustomer.getMetaData();
                        int numberOfColumns = newCustomerData.getColumnCount();
                        for (int i = 1; i <= numberOfColumns; i++) {
                            System.out.printf("%-15s", newCustomerData.getColumnName(i));
                        }
                        System.out.println("\n" + "-".repeat(15 * numberOfColumns));
                        while (rsNewlyAddedCustomer.next()) {
                            for (int i = 1; i <= numberOfColumns; i++) {
                                Object value = rsNewlyAddedCustomer.getObject(i);
                                System.out.printf("%-15s", value != null ? value.toString() : "NULL");
                            }
                            System.out.println();
                        }
                    }
                }
                catch (Exception e){
                    System.out.println(e.getMessage());
                }
                postStatement.close();

                /**************************updating a specific row in a specific table****************************/
                PreparedStatement putstatement = connection.prepareStatement("""
                UPDATE customer SET address = ?, postal_code = ? WHERE id = ?""");

                putstatement.setString(1,"Storgatan 4");
                putstatement.setString(2,"666 23");
                putstatement.setInt(3,2);
                int rowUpdated = putstatement.executeUpdate();
                System.out.println("\n----------------updating a specific row in a specific table----------------");
                System.out.println("Row updated: "+ rowUpdated);
                putstatement.close();


                /*********************fetching a specific row from a table which holds several rows*********************/
                PreparedStatement getSpecificRowStatement = connection.prepareStatement(
                        """
                                SELECT * FROM order_head WHERE id = ?
                            """);
                getSpecificRowStatement.setInt(1,2);
                ResultSet rsGetSpefificRow = getSpecificRowStatement.executeQuery();
                System.out.println("\n----------------fetching a specific row from a table which holds several rows----------------");
                if (rsGetSpefificRow.next()){
                    System.out.println("Order number 2 was made on "+rsGetSpefificRow.getString("order_date"));
                }

                /***********************joining tables*************************/
                PreparedStatement joinStatement = connection.prepareStatement("""
                SELECT oh.customer_id, oh.id, ol.furniture_id, ol.quantity, f.color, f.name,f.price FROM order_head as oh
                JOIN order_line as ol ON oh.id = ol.order_id
                JOIN furniture as f ON ol.furniture_id = f.id 
                WHERE oh.id = ?
                """);
                joinStatement.setInt(1,2);
                ResultSet rsJoin= joinStatement.executeQuery();
                ResultSetMetaData joinData = rsJoin.getMetaData();
                int columnCount = joinData.getColumnCount();
                System.out.println("\n----------------fetching data from several tables by using a join or an adequate where-clause----------------");
                System.out.println("Fetching the orders made by customer_id 2");
                for (int i = 1; i <= columnCount; i++) {
                    System.out.printf("%-15s", joinData.getColumnName(i));
                }
                System.out.println("\n" + "-".repeat(15 * columnCount));
                while (rsJoin.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rsJoin.getObject(i);
                        System.out.printf("%-15s", value != null ? value.toString() : "NULL");
                    }
                    System.out.println();
                }
                rsJoin.close();
                joinStatement.close();


            } catch (SQLException e) {
                System.err.println("Connection failed: " + e.getMessage());
                e.printStackTrace();
            }

        }
        catch(IOException e){
            System.err.println("Error loading db.properties: " + e.getMessage());
            e.printStackTrace();
        }

    }
}