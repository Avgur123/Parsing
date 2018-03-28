package parser;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class BondParser {


    public static DataSource hikariDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setMaximumPoolSize(20);
        dataSource.setDataSourceClassName("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
        dataSource.addDataSourceProperty("url", "jdbc:sqlserver://mosexchange1.prime.local:1433;DatabaseName=Info");
        dataSource.addDataSourceProperty("user", "inter-server");
        dataSource.addDataSourceProperty("password", "19876");
        dataSource.setPoolName("springHikariPool");
        return dataSource;
    }

    public static void main(String[] args) {




        String query = "INSERT INTO dbo.consensus (ticker, source, author, recommend, targetprice, date) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection connection = hikariDataSource().getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);

            excelData.forEach((s, maps) -> {
                maps.forEach(stringObjectMap -> {
                    try {
                        statement.setString(1, String.valueOf(stringObjectMap.get("")));
                        statement.execute();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            });
        } catch (SQLException e) {
            e.printStackTrace();
        }




    }
}
