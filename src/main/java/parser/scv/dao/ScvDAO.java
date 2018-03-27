package parser.scv.dao;

import java.util.Set;

public interface ScvDAO {
    Set<String> getEndOfDayWrittenFiles();
    void proceedEndOfDayReport();
    Set<String> getRepoCKWrittenFiles();
    void proceedRepoCKReport();
    Set<String> getRPCWrittenFiles();
    void proceedRPCReport();
}
