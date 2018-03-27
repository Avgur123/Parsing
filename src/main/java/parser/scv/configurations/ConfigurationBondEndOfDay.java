package parser.scv.configurations;

import parser.scv.afterjob.AfterJobBondEndOfDay;
import parser.scv.dao.ScvDAO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.core.io.Resource;
import parser.scv.mappers.FieldSetMapperBondEndOfDay;
import parser.scv.models.EndOfDay;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@EnableBatchProcessing
@Configuration
public class ConfigurationBondEndOfDay {

    @Value("file://VMCOLLECTOR2/DataIn/mmvb_bonds_*.csv")
    public Resource[] bondEndOfDayResources;

    @Autowired
    public ScvDAO dao;

    @Autowired
    public JobBuilderFactory jobs;

    @Autowired
    public StepBuilderFactory steps;

    @Autowired
    @Qualifier("hikariDataSource")
    public DataSource dataSource;

    @Autowired
    public AfterJobBondEndOfDay afterJobBondEndOfDay;

    @Bean
    public Job bondEndOfDayJob() {
        return jobs.get("bondEndOfDayJob")
                .incrementer(new RunIdIncrementer())
                .flow(bondEndOfDayStep())
                .end()
                .listener(afterJobBondEndOfDay)
                .build();
    }

    @Bean
    public Step bondEndOfDayStep() {
        return steps.get("bondEndOfDayStep")
                .<EndOfDay, EndOfDay> chunk(1000)
                .reader(bondEndOfDayMultiResourceReader())
                .writer(bondEndOfDayWriter(dataSource))
                .build();
    }

    @Bean
    public MultiResourceItemReader<EndOfDay> bondEndOfDayMultiResourceReader() {
        List<Resource> resourcesToDb = new ArrayList<>();
        Set<String> files = dao.getEndOfDayWrittenFiles();

        for (Resource bondEndOfDayResource : bondEndOfDayResources) {
            boolean contains = false;
            String currentResource = bondEndOfDayResource.toString();
            for (String file : files) {
                if (currentResource.substring(currentResource.indexOf("mmvb", 0), currentResource.indexOf("]")).equals(file)) {
                    contains = true;
                }
            }
            if (!contains) {
                resourcesToDb.add(bondEndOfDayResource);
            }
        }
        Resource[] resourcesToDbArray = resourcesToDb.toArray(new Resource[resourcesToDb.size()]);
        MultiResourceItemReader<EndOfDay> multi = new MultiResourceItemReader<>();
        multi.setResources(resourcesToDbArray);
        multi.setDelegate(bondEndOfDayReader());
        return multi;
    }

    @Bean
    public FlatFileItemReader<EndOfDay> bondEndOfDayReader() {
        FlatFileItemReader<EndOfDay> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper<EndOfDay>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setDelimiter(";");
                setIncludedFields(new int[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49});
                setFieldSetMapper(new FieldSetMapperBondEndOfDay());
                setNames(new String[]{
                        "tradeDate", "secId", "boardName", "boardId", "shortName", "regNumber", "isin", "matDate",
                        "listName", "faceValue", "currencyId", "volume", "value", "numTrades", "prevPrice",
                        "prevLegalClosePrice", "openPeriod", "openPrice", "legalOpenPrice", "low", "high",
                        "legalClosePrice", "closePrice", "closePeriod", "waPrice", "trendClose", "trendClsPr", "trendWap", "trendWapPr",
                        "openVal", "closeVal", "highBid", "lowOffer", "bid", "offer", "yieldAtWap", "yieldClose",
                        "accint", "duration", "marketPrice", "marketPrice2", "admittedQuote", "issueSize", "mpvaltrd",
                        "mp2valtrd", "admittedValue", "marketPrice3", "marketPrice3TradesValue", "decimals", "type"});
            }});
        }});
        return reader;
    }

    @Bean
    @Autowired
    public JdbcBatchItemWriter<EndOfDay> bondEndOfDayWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<EndOfDay>()
                .dataSource(dataSource)
                .sql("INSERT INTO dbo.bonds_endofdate (" +
                        " tradeDate, secId, boardName, boardId, shortName, regNumber, isin, matDate, " +
                        " listName, faceValue, currencyId, volume, value, numTrades, prevPrice," +
                        " prevLegalClosePrice, openPeriod, openPrice, legalOpenPrice, low, high," +
                        " legalClosePrice, closePrice, closePeriod, waPrice, trendClose, trendClsPr, trendWap, trendWapPr," +
                        " openVal, closeVal, highBid, lowOffer, bid, offer, yieldAtWap, yieldClose," +
                        " accint, duration, marketPrice, marketPrice2, admittedQuote, issueSize, mpvaltrd," +
                        " mp2valtrd, admittedValue, marketPrice3, marketPrice3TradesValue, decimals, type) " +
                        " VALUES (" +
                        " :tradeDate, :secId, :boardName, :boardId, :shortName, :regNumber, :isin, :matDate," +
                        " :listName, :faceValue, :currencyId, :volume, :value, :numTrades, :prevPrice," +
                        " :prevLegalClosePrice, :openPeriod, :openPrice, :legalOpenPrice, :low, :high," +
                        " :legalClosePrice, :closePrice, :closePeriod, :waPrice, :trendClose, :trendClsPr, :trendWap, :trendWapPr," +
                        " :openVal, :closeVal, :highBid, :lowOffer, :bid, :offer, :yieldAtWap, :yieldClose," +
                        " :accint, :duration, :marketPrice, :marketPrice2, :admittedQuote, :issueSize, :mpvaltrd," +
                        " :mp2valtrd, :admittedValue, :marketPrice3, :marketPrice3TradesValue, :decimals, :type)")
                .beanMapped()
                .build();
    }
}