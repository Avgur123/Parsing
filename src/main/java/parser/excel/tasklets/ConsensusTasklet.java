package parser.excel.tasklets;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ConsensusTasklet implements Tasklet {


    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

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




        return null;
    }
}
