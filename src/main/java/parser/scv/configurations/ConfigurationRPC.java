package parser.scv.configurations;

import parser.scv.afterjob.AfterJobRPC;
import parser.scv.dao.ScvDAO;
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
import parser.scv.mappers.FieldSetMapperRPC;
import parser.scv.models.Rpc;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@EnableBatchProcessing
@Configuration
public class ConfigurationRPC {

    @Value("file://VMCOLLECTOR2/DataIn/mmvb_rpc_*.csv")
    public Resource[] RPCResources;

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
    public AfterJobRPC afterJobRPC;

    @Bean
    public Job rpcJob() {
        return jobs.get("rpcJob")
                .incrementer(new RunIdIncrementer())
                .flow(rpcStep())
                .end()
                .listener(afterJobRPC)
                .build();
    }

    @Bean
    public Step rpcStep() {
        return steps.get("rpcStep")
                .<Rpc, Rpc> chunk(1000)
                .reader(rpcMultiResourceReader())
                .writer(rpcWriter(dataSource))
                .build();
    }

    @Bean
    public MultiResourceItemReader<Rpc> rpcMultiResourceReader() {
        List<Resource> resourcesToDb = new ArrayList<>();
        Set<String> files = dao.getRPCWrittenFiles();

        for (Resource bondEndOfDayResource : RPCResources) {
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
        MultiResourceItemReader<Rpc> multi = new MultiResourceItemReader<>();
        multi.setResources(resourcesToDbArray);
        multi.setDelegate(rpcReader());
        return multi;
    }

    @Bean
    public FlatFileItemReader<Rpc> rpcReader() {
        FlatFileItemReader<Rpc> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper<Rpc>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setDelimiter(";");
                setIncludedFields(new int[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60});
                setFieldSetMapper(new FieldSetMapperRPC());
                setNames(new String[]{
                        "tradeDate", "secId", "boardName", "boardId", "shortName", "regNumber", "isin", "matDate",
                        "listName", "faceValue", "currencyId", "volume", "value", "numTrades", "prevPrice",
                        "openPrice", "lowPrice", "highPrice",
                        "closePrice", "waPrice", "trendClose", "openVal", "closeVal", "trendWap", "trendClsPr", "trendWapPr",
                        "yieldAtWap", "yieldClose", "accint", "issueSize", "decimals", "type", "iricpiClose",
                        "beiClose", "couponPercent", "couponValue", "buyBackDate", "lastTradeDate", "cbrClose",
                        "yieldToOffer", "yieldLastCoupon", "offerDate", "faceUnit"});
            }});
        }});
        return reader;
    }

    @Bean
    @Autowired
    public JdbcBatchItemWriter<Rpc> rpcWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Rpc>()
                .dataSource(dataSource)
                .sql("INSERT INTO dbo.rpc (" +
                        " tradeDate, secId, boardName, boardId, shortName, regNumber, isin, matDate, " +
                        " listName, faceValue, currencyId, volume, value, numTrades, prevPrice," +
                        " openPrice, lowPrice, highPrice," +
                        " closePrice, waPrice, trendClose, openVal, closeVal, trendWap, trendClsPr, trendWapPr," +
                        " yieldAtWap, yieldClose, accint, issueSize, decimals, type, iricpiClose," +
                        " beiClose, couponPercent, couponValue, buyBackDate, lastTradeDate, cbrClose," +
                        " yieldToOffer, yieldLastCoupon, offerDate, faceUnit) " +
                        " VALUES (" +
                        " :tradeDate, :secId, :boardName, :boardId, :shortName, :regNumber, :isin, :matDate," +
                        " :listName, :faceValue, :currencyId, :volume, :value, :numTrades, :prevPrice," +
                        " :openPrice, :lowPrice, :highPrice," +
                        " :closePrice, :waPrice, :trendClose, :openVal, :closeVal, :trendWap, :trendClsPr, :trendWapPr," +
                        " :yieldAtWap, :yieldClose, :accint, :issueSize, :decimals, :type, :iricpiClose," +
                        " :beiClose, :couponPercent, :couponValue, :buyBackDate, :lastTradeDate, :cbrClose," +
                        " :yieldToOffer, :yieldLastCoupon, :offerDate, :faceUnit)")
                .beanMapped()
                .build();
    }
}
