package parser.scv.dao;

import parser.scv.FilesDataCollector;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

//TODO logging

@Repository
public class ScvDAOImpl implements ScvDAO {

    @Autowired
    @Qualifier("hikariDataSource")
    private DataSource dataSource;

    private Set<String> getWrittenFiles(String parserKey) {
        Set<String> files = new HashSet();
        String query = "SELECT file_name FROM dbo.parser_report " +
                "WHERE parser_key = '" + parserKey + "'";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                files.add(resultSet.getString("file_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return files;
    }

    @Override
    public Set<String> getEndOfDayWrittenFiles() {
        return getWrittenFiles("bondEOD");
    }

    @Override
    public Set<String> getRepoCKWrittenFiles() {
        return getWrittenFiles("repoCK");
    }

    @Override
    public Set<String> getRPCWrittenFiles() {
        return getWrittenFiles("rpc");
    }

    @Override
    public void proceedEndOfDayReport() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String query = "INSERT INTO dbo.parser_report (parser_key, file_name, error_message) VALUES (?, ?, ?)";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            FilesDataCollector.getCollector()
                    .getFullErrorLogMapBondEndOfDate()
                    .forEach((date, stringMapMap) -> {
                        try {
                            statement.setString(1, "bondEOD");
                            statement.setString(2, "mmvb_bonds_" + format.format(date) + ".csv");
                            statement.setString(3, new ObjectMapper().writeValueAsString(stringMapMap));
                            statement.execute();
                        } catch (SQLException | JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void proceedRepoCKReport() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String query = "INSERT INTO dbo.parser_report (parser_key, file_name, error_message) VALUES (?, ?, ?)";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            FilesDataCollector.getCollector()
                    .getFullErrorLogMapRepoCK()
                    .forEach((date, stringMapMap) -> {
                        try {
                            statement.setString(1, "repoCK");
                            statement.setString(2, "mmvb_repo_ck_" + format.format(date) + ".csv");
                            statement.setString(3, new ObjectMapper().writeValueAsString(stringMapMap));
                            statement.execute();
                        } catch (SQLException | JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void proceedRPCReport() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String query = "INSERT INTO dbo.parser_report (parser_key, file_name, error_message) VALUES (?, ?, ?)";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            FilesDataCollector.getCollector()
                    .getFullErrorLogMapRPC()
                    .forEach((date, stringMapMap) -> {
                        try {
                            statement.setString(1, "rpc");
                            statement.setString(2, "mmvb_rpc_" + format.format(date) + ".csv");
                            statement.setString(3, new ObjectMapper().writeValueAsString(stringMapMap));
                            statement.execute();
                        } catch (SQLException | JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}