package parser;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class HikariConfiguration {

    @Bean
    @Primary
    @ConfigurationProperties(prefix = "datasource.infoserver")
    public DataSourceProperties dataSourceProperties(){
        return new DataSourceProperties();
    }

    @Bean(name = "hikariDataSource")
    @Primary
    public DataSource hikariDataSource() {
        DataSourceProperties dataSourceProperties = dataSourceProperties();
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setMaximumPoolSize(20);
        dataSource.setDataSourceClassName(dataSourceProperties.getDriverClassName());
        dataSource.addDataSourceProperty("url", dataSourceProperties.getUrl());
        dataSource.addDataSourceProperty("user", dataSourceProperties.getUsername());
        dataSource.addDataSourceProperty("password", dataSourceProperties.getPassword());
        dataSource.setPoolName("springHikariPool");
        return dataSource;
    }
}
