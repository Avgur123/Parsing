package parser.excel.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import parser.excel.models.Director;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Repository
public class ExcelDAOImpl implements ExcelDAO {

    @Autowired
    @Qualifier("hikariDataSource")
    public DataSource dataSource;

    @Override
    public void writeDirectorDataToDb(List<Director> directorModels) {
        String query = "INSERT INTO dbo.directors_ejo (EmitentName, Inn, Date, Status, ManagingAuthority, " +
                "DirectorName, Birthday, Education, IsChairman, PeriodFrom, PeriodTo, CompanyName, Position, " +
                "CapitalShare, SequrityShare, Relations, Crime, CrimeJob) VALUES (?, ?, ?, ?, ?, ? ,? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);

            directorModels.forEach(model -> {
                try {
                    statement.setString(1, model.getEmitentName());
                    statement.setLong(2, model.getInn());

                    java.util.Date utilDate = model.getDate();
                    Date sqlDate = new Date(utilDate.getTime());
                    statement.setDate(3, sqlDate);

                    statement.setString(4, model.getStatus());
                    statement.setString(5, model.getManagingAuthority());
                    statement.setString(6, model.getDirectorName());
                    try {
                        statement.setInt(7, model.getBirthday());
                    } catch (NullPointerException e) {}

                    statement.setString(8, model.getEducation());
                    statement.setString(9, model.getIsChairman());
                    statement.setString(10, model.getPeriodFrom());
                    statement.setString(11, model.getPeriodTo());
                    statement.setString(12, model.getCompanyName());
                    statement.setString(13, model.getPosition());
                    try {
                        statement.setFloat(14, model.getCapitalShare());
                    } catch (NullPointerException e) {}

                    try {
                        statement.setFloat(15, model.getSequrityShare());
                    } catch (NullPointerException e) {}

                    statement.setString(16, model.getRelations());
                    statement.setString(17, model.getCrime());
                    statement.setString(18, model.getCrimeJob());
                    statement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
