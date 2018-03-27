package parser.scv;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class FilesDataCollector {

    private static volatile FilesDataCollector collector;

    private Map<Date, Map<String, Map<String, Object>>> fullErrorLogMapBondEndOfDate = new HashMap<>();
    private Map<Date, Map<String, Map<String, Object>>> fullErrorLogMapRepoCK = new HashMap<>();
    private Map<Date, Map<String, Map<String, Object>>> fullErrorLogMapRPC = new HashMap<>();

    private FilesDataCollector() {}

    public static FilesDataCollector getCollector() {
        if (collector == null) {
            synchronized (FilesDataCollector.class) {
                if (collector == null) {
                    collector = new FilesDataCollector();
                }
            }
        }
        return collector;
    }

    public Map<Date, Map<String, Map<String, Object>>> getFullErrorLogMapBondEndOfDate() {
        return fullErrorLogMapBondEndOfDate;
    }

    public Map<Date, Map<String, Map<String, Object>>> getFullErrorLogMapRepoCK() {
        return fullErrorLogMapRepoCK;
    }

    public Map<Date, Map<String, Map<String, Object>>> getFullErrorLogMapRPC() {
        return fullErrorLogMapRPC;
    }
}
