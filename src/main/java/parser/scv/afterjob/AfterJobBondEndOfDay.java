package parser.scv.afterjob;

import parser.scv.dao.ScvDAO;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AfterJobBondEndOfDay extends JobExecutionListenerSupport {

    @Autowired
    private ScvDAO dao;

    @Override
    public void afterJob(JobExecution jobExecution) {
        dao.proceedEndOfDayReport();
    }
}