package parser;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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

        public DataSource dataSource;

        Map<String, List<Map<String, Object>>> excelData = new HashMap<>();

        XSSFWorkbook excelBook = new XSSFWorkbook(new FileInputStream("\\\\VMCOLLECTOR2\\DataIn\\тикеры -  2000-3415.xlsm"));



        for (int i = 0; i < excelBook.getNumberOfSheets(); i++) {
            List<Map<String, Object>> currentSheetData = new ArrayList<>();

            XSSFSheet sheet = excelBook.getSheetAt(i);
            XSSFRow firstRow = sheet.getRow(1);

            if (firstRow.getCell(1).getStringCellValue().contains("RU Equity")) {

                for (int row = 4; row <= sheet.getLastRowNum(); row++) {

                    Map<String, Object> currentRowData = new HashMap<>();
                    currentRowData.put("ticker", firstRow.getCell(1).toString().substring(0, firstRow.getCell(1).toString().indexOf("RU") - 1));


                    if (sheet.getRow(row).getCell(2).toString().isEmpty()) {
                        break;
                    }
                    for (int cell = 2; cell <= 9; cell++) {
                        try {
                            if (cell == 2) {
                                currentRowData.put("source", sheet.getRow(row).getCell(cell));
                            }
                            if (cell == 3) {
                                currentRowData.put("author", sheet.getRow(row).getCell(cell));
                            }
                            if (cell == 4) {
                                currentRowData.put("recommend", sheet.getRow(row).getCell(cell));
                            }
                            if (cell == 7) {
                                if (sheet.getRow(row).getCell(cell).toString().equals("#N/A N/A")) {
                                    currentRowData.put("targetprice", 0);
                                } else {
                                    currentRowData.put("targetprice", sheet.getRow(row).getCell(cell));
                                }
                            }
                            if (cell == 9) {
                                currentRowData.put("date", sheet.getRow(row).getCell(cell).getDateCellValue());
                            }
                        } catch (NullPointerException ignored) {}

                    }
                    currentSheetData.add(currentRowData);
                }
            }
            excelData.put(firstRow.getCell(1).toString(), currentSheetData);
        }




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
