package parser.scv.configurations;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import parser.scv.afterjob.AfterJobRepoCK;
import parser.scv.dao.ScvDAO;
import parser.scv.mappers.FieldSetMapperRepoCK;
import parser.scv.models.RepoCk;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@EnableBatchProcessing
@Configuration
public class ConfigurationRepoCK {

    @Value("file://VMCOLLECTOR2/DataIn/mmvb_repo_ck_*.csv")
    public Resource[] repoCKResources;

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
    public AfterJobRepoCK afterJobRepoCK;

    @Bean
    public Job repoCKJob() {
        return jobs.get("RepoCKJob")
                .incrementer(new RunIdIncrementer())
                .flow(repoCKStep())
                .end()
                .listener(afterJobRepoCK)
                .build();
    }

    @Bean
    public Step repoCKStep() {
        return steps.get("repoCKStep")
                .<RepoCk, RepoCk> chunk(1000)
                .reader(repoCKMultiResourceReader())
                .writer(repoCKWriter(dataSource))
                .build();
    }

    @Bean
    public MultiResourceItemReader<RepoCk> repoCKMultiResourceReader() {
        List<Resource> resourcesToDb = new ArrayList<>();
        Set<String> files = dao.getRepoCKWrittenFiles();

        for (Resource repoCKResource : repoCKResources) {
            boolean contains = false;
            String currentResource = repoCKResource.toString();
            for (String file : files) {
                if (currentResource.substring(currentResource.indexOf("mmvb", 0), currentResource.indexOf("]")).equals(file)) {
                    contains = true;
                }
            }
            if (!contains) {
                resourcesToDb.add(repoCKResource);
            }
        }
        Resource[] resourcesToDbArray = resourcesToDb.toArray(new Resource[resourcesToDb.size()]);
        MultiResourceItemReader<RepoCk> multi = new MultiResourceItemReader<>();
        multi.setResources(resourcesToDbArray);
        multi.setDelegate(repoCKReader());
        return multi;
    }

    @Bean
    public FlatFileItemReader<RepoCk> repoCKReader() {
        FlatFileItemReader<RepoCk> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper<RepoCk>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setDelimiter(";");
                setIncludedFields(new int[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49});
                setFieldSetMapper(new FieldSetMapperRepoCK());
                setNames(new String[]{
                        "tradeDate", "secId", "boardName", "boardId", "shortName", "regNumber", "isin", "listName",
                        "matDate", "faceValue", "currencyId", "volume", "value", "numTrades", "prevPrice",
                        "prevLegalClosePrice", "openPeriod", "openPrice", "legalOpenPrice", "low", "high",
                        "legalClosePrice", "closePrice", "closePeriod", "waPrice", "trendClose", "trendClsPr", "trendWap", "trendWapPr",
                        "openVal", "closeVal", "highBid", "lowOffer", "bid", "offer", "yieldAtWap", "yieldClose",
                        "accint", "duration", "marketPrice", "marketPrice2", "admittedQuote", "issueSize", "marketPrice3",
                        "marketPrice3TradesValue", "mpvaltrd", "mp2valtrd", "admittedValue", "decimals", "type"});
            }});
        }});
        return reader;
    }

    @Bean
    @Autowired
    public JdbcBatchItemWriter<RepoCk> repoCKWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<RepoCk>()
                .dataSource(dataSource)
                .sql("INSERT INTO dbo.repo_ck (" +
                        " tradeDate, secId, boardName, boardId, shortName, regNumber, isin, listName, " +
                        " matDate, faceValue, currencyId, volume, value, numTrades, prevPrice," +
                        " prevLegalClosePrice, openPeriod, openPrice, legalOpenPrice, low, high," +
                        " legalClosePrice, closePrice, closePeriod, waPrice, trendClose, trendClsPr, trendWap, trendWapPr," +
                        " openVal, closeVal, highBid, lowOffer, bid, offer, yieldAtWap, yieldClose," +
                        " accint, duration, marketPrice, marketPrice2, admittedQuote, marketPrice3, marketPrice3TradesValue, " +
                        " issueSize, mpvaltrd, mp2valtrd, admittedValue,  decimals, type) " +
                        " VALUES (" +
                        " :tradeDate, :secId, :boardName, :boardId, :shortName, :regNumber, :isin, :listName," +
                        " :matDate, :faceValue, :currencyId, :volume, :value, :numTrades, :prevPrice," +
                        " :prevLegalClosePrice, :openPeriod, :openPrice, :legalOpenPrice, :low, :high," +
                        " :legalClosePrice, :closePrice, :closePeriod, :waPrice, :trendClose, :trendClsPr, :trendWap, :trendWapPr," +
                        " :openVal, :closeVal, :highBid, :lowOffer, :bid, :offer, :yieldAtWap, :yieldClose," +
                        " :accint, :duration, :marketPrice, :marketPrice2, :admittedQuote, :marketPrice3, :marketPrice3TradesValue, :issueSize, " +
                        " :mpvaltrd, :mp2valtrd, :admittedValue,  :decimals, :type)")
                .beanMapped()
                .build();
    }
}
