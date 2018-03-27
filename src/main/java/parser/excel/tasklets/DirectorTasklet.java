package parser.excel.tasklets;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import parser.excel.dao.ExcelDAO;
import parser.excel.models.Director;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class DirectorTasklet implements Tasklet {

    @Autowired
    @Qualifier("hikariDataSource")
    public DataSource dataSource;

    @Autowired
    public ExcelDAO dao;

    private List<Director> directorData = new ArrayList<>();

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

        try (FileInputStream inputStream = new FileInputStream("\\\\VMCOLLECTOR2\\DataIn\\Органы управления 201706_20180124.xlsx")) {
            XSSFWorkbook excelBook = new XSSFWorkbook(inputStream);
            XSSFSheet sheet = excelBook.getSheetAt(0);

            for (int rowNum = 1; rowNum < sheet.getLastRowNum(); rowNum++) {
                Director director = new Director();
                for (int columnNum = 0; columnNum <= 17; columnNum++) {

                    if (columnNum == 0) {
                        director.setEmitentName(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                    }
                    if (columnNum == 1) {
                        director.setInn(Long.valueOf(sheet.getRow(rowNum).getCell(columnNum).getRawValue()));
                    }
                    if (columnNum == 2) {
                        director.setDate(sheet.getRow(rowNum).getCell(columnNum).getDateCellValue());
                    }
                    if (columnNum == 3) {
                        director.setStatus(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                    }
                    if (columnNum == 4) {
                        director.setManagingAuthority(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                    }
                    if (columnNum == 5) {
                        try {
                            director.setDirectorName(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setDirectorName(null);
                        }
                    }
                    if (columnNum == 6) {
                        try {
                            director.setBirthday(Integer.valueOf(sheet.getRow(rowNum).getCell(columnNum).getRawValue()));
                        } catch (NullPointerException | NumberFormatException e) {
                            director.setBirthday(null);
                        }
                    }
                    if (columnNum == 7) {
                        try {
                            director.setEducation(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setEducation(null);
                        }
                    }
                    if (columnNum == 8) {
                        try {
                            director.setIsChairman(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setIsChairman(null);
                        }
                    }
                    if (columnNum == 9) {
                        try {
                            director.setPeriodFrom(String.valueOf(sheet.getRow(rowNum).getCell(columnNum).getRawValue()));
                        } catch (NullPointerException e) {
                            director.setPeriodFrom(null);
                        }
                    }
                    if (columnNum == 10) {
                        try {
                            director.setPeriodTo(String.valueOf(sheet.getRow(rowNum).getCell(columnNum).getRawValue()));
                        } catch (NullPointerException e) {
                            director.setPeriodTo(null);
                        }
                    }
                    if (columnNum == 11) {
                        try {
                            director.setCompanyName(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setCompanyName(null);
                        }
                    }
                    if (columnNum == 12) {
                        try {
                            director.setPosition(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setPosition(null);
                        }
                    }
                    if (columnNum == 13) {
                        try {
                            director.setCapitalShare(Float.valueOf(sheet.getRow(rowNum).getCell(columnNum).getRawValue().replace(',', '.')));
                        } catch (NullPointerException e) {
                            director.setCapitalShare(null);
                        }
                    }
                    if (columnNum == 14) {
                        try {
                            director.setSequrityShare(Float.valueOf(sheet.getRow(rowNum).getCell(columnNum).getRawValue().replace(',', '.')));
                        } catch (NullPointerException e) {
                            director.setSequrityShare(null);
                        }
                    }
                    if (columnNum == 15) {
                        try {
                            director.setRelations(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setRelations(null);
                        }
                    }
                    if (columnNum == 16) {
                        try {
                            director.setCrime(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setCrime(null);
                        }
                    }
                    if (columnNum == 17) {
                        try {
                            director.setCrimeJob(sheet.getRow(rowNum).getCell(columnNum).getStringCellValue());
                        } catch (NullPointerException e) {
                            director.setCrimeJob(null);
                        }
                    }
                }
                directorData.add(director);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        dao.writeDirectorDataToDb(directorData);
        return null;
    }
}
