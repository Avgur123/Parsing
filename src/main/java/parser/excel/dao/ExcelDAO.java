package parser.excel.dao;

import parser.excel.models.Director;

import java.util.List;

public interface ExcelDAO {
    void writeDirectorDataToDb(List<Director> directorModels);
}
