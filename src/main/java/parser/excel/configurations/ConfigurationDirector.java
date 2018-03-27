package parser.excel.configurations;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import parser.excel.tasklets.DirectorTasklet;

/*@EnableBatchProcessing
@Configuration*/
public class ConfigurationDirector {

    @Autowired
    public JobBuilderFactory jobs;

    @Autowired
    public StepBuilderFactory steps;

    @Autowired
    public DirectorTasklet directorTasklet;

    @Bean
    public Job directorJob() {
        return jobs.get("directorJob")
                .incrementer(new RunIdIncrementer())
                .start(directorStep())
                .build();
    }

    @Bean
    public Step directorStep() {
        return steps.get("directorStep")
                .tasklet(directorTasklet)
                .build();
    }
}
