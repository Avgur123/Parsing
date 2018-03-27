package parser;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Scheduled;

/*@Configuration
@EnableScheduling*/
public class Scheduler {

    @Autowired
    public JobLauncher launcher;

    @Autowired
    public Job scvJob;

   /* @Bean
    public Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("location", "application.yml");
        return properties;
    }
*/

    @Bean
    public PropertyPlaceholderConfigurer placeholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setBeanName("location");
        configurer.setLocations(new ClassPathResource("cron.schedule.time"));
        return configurer;
    }

    @Scheduled(cron = "${placeholderConfigurer}")
    public void sheduleParsing() {
        System.out.println("Test");
        JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis()).toJobParameters();
        try {
            launcher.run(scvJob, jobParameters);

        } catch (JobExecutionAlreadyRunningException | JobParametersInvalidException | JobInstanceAlreadyCompleteException | JobRestartException e) {
            e.printStackTrace();
        }
    }
}
