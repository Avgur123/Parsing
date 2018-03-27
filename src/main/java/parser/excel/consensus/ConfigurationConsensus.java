package parser.excel.consensus;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import parser.excel.tasklets.ConsensusTasklet;

public class ConfigurationConsensus {

    @Autowired
    private ConsensusTasklet consensusTasklet;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step consensusStep() {
        return stepBuilderFactory.get("consensusStep")
                .tasklet(consensusTasklet)
                .build();
    }

    @Bean
    public Job consensusJob() {
        return jobBuilderFactory.get("consensusJob")
                .incrementer(new RunIdIncrementer())
                .flow(consensusStep())
                .end()
                .build();
    }
}