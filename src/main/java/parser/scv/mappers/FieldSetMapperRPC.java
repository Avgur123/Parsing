package parser.scv.mappers;

import parser.scv.FilesDataCollector;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;
import parser.scv.models.Rpc;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class FieldSetMapperRPC implements FieldSetMapper {
    private FilesDataCollector errorCollector = FilesDataCollector.getCollector();

    private Map<String, Map<String, Object>> errors = new HashMap<>();

    private SimpleDateFormat parserFormat = new SimpleDateFormat("dd.MM.yyyy");
    private DecimalFormatSymbols decimalSymbols = new DecimalFormatSymbols();
    private DecimalFormat decimalFormat = new DecimalFormat("0.#");

    @Override
    public Object mapFieldSet(FieldSet fieldSet) throws BindException {
        Rpc bond = new Rpc();
        Date dateFromFile;

        Map<String, Object> errorModel = new HashMap<>();

        decimalSymbols.setDecimalSeparator('.');
        decimalFormat.setDecimalFormatSymbols(decimalSymbols);

        try {
            dateFromFile = parserFormat.parse(fieldSet.readString("tradeDate"));
        } catch (ParseException e) {
            dateFromFile = null;
            errorModel.put("tradeDate", e.getMessage());
        }
        bond.setTradeDate(dateFromFile);
        try {
            dateFromFile = parserFormat.parse(fieldSet.readString("matDate"));
        } catch (ParseException e) {
            dateFromFile = null;
            errorModel.put("matDate", e.getMessage());
        }
        bond.setMatDate(dateFromFile);
        try {
            dateFromFile = parserFormat.parse(fieldSet.readString("buyBackDate"));
        } catch (ParseException e) {
            dateFromFile = null;
            errorModel.put("buyBackDate", e.getMessage());
        }
        bond.setBuyBackDate(dateFromFile);
        try {
            dateFromFile = parserFormat.parse(fieldSet.readString("lastTradeDate"));
        } catch (ParseException e) {
            dateFromFile = null;
            errorModel.put("lastTradeDate", e.getMessage());
        }
        bond.setLastTradeDate(dateFromFile);
        try {
            dateFromFile = parserFormat.parse(fieldSet.readString("offerDate"));
        } catch (ParseException e) {
            dateFromFile = null;
            if (e.getMessage().equals(""))
            errorModel.put("offerDate", e.getMessage());
        }
        bond.setOfferDate(dateFromFile);

        bond.setSecId(fieldSet.readRawString("secId"));
        bond.setBoardName(fieldSet.readRawString("boardName"));
        bond.setBoardId(fieldSet.readRawString("boardId"));
        bond.setShortName(fieldSet.readRawString("shortName"));
        bond.setRegNumber(fieldSet.readRawString("regNumber"));
        bond.setIsin(fieldSet.readRawString("isin"));
        bond.setCurrencyId(fieldSet.readRawString("currencyId"));
        bond.setType(fieldSet.readRawString("type"));
        bond.setFaceUnit(fieldSet.readRawString("faceUnit"));

        try{
            bond.setDecimals(fieldSet.readInt("decimals"));
        } catch (NumberFormatException e) {
            if (!e.getMessage().equals("Unparseable number: ")) {
                errorModel.put("decimals", e.getMessage());
            }
        }
        try{
            bond.setIssueSize(fieldSet.readLong("issueSize"));
        } catch (NumberFormatException e) {
            if (!e.getMessage().equals("Unparseable number: ")) {
                errorModel.put("issueSize", e.getMessage());
            }
        }
        try{
            bond.setNumTrades(fieldSet.readInt("numTrades"));
        } catch (NumberFormatException e) {
            if (!e.getMessage().equals("Unparseable number: ")) {
                errorModel.put("numTrades", e.getMessage());
            }
        }
        try{

            bond.setVolume(fieldSet.readInt("volume"));
        } catch (NumberFormatException e) {
            if (!e.getMessage().equals("Unparseable number: ")) {
                errorModel.put("volume", e.getMessage());
            }
        }
        try{

            bond.setListName(fieldSet.readInt("listName"));
        } catch (NumberFormatException e) {
            if (!e.getMessage().equals("Unparseable number: ")) {
                errorModel.put("listName", e.getMessage());
            }
        }
        try {

            bond.setFaceValue(decimalFormat.parse(fieldSet.readRawString("faceValue").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("faceValue", e.getMessage());
            }
        }
        try {

            bond.setValue(decimalFormat.parse(fieldSet.readRawString("value").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("value", e.getMessage());
            }
        }
        try {
            bond.setPrevPrice(decimalFormat.parse(fieldSet.readRawString("prevPrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("prevPrice", e.getMessage());
            }
        }
        try {
            bond.setOpenPrice(decimalFormat.parse(fieldSet.readRawString("openPrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("openPrice", e.getMessage());
            }
        }
        try {
            bond.setOpenPrice(decimalFormat.parse(fieldSet.readRawString("lowPrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("lowPrice", e.getMessage());
            }
        }
        try {
            bond.setOpenPrice(decimalFormat.parse(fieldSet.readRawString("highPrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("highPrice", e.getMessage());
            }
        }
        try {
            bond.setClosePrice(decimalFormat.parse(fieldSet.readRawString("closePrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("closePrice", e.getMessage());
            }
        }
        try {

            bond.setWaPrice(decimalFormat.parse(fieldSet.readRawString("waPrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("waPrice", e.getMessage());
            }
        }

        try {
            bond.setTrendClose(decimalFormat.parse(fieldSet.readRawString("trendClose").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("trendClose", e.getMessage());
            }
        }
        try {
            bond.setOpenVal(decimalFormat.parse(fieldSet.readRawString("openVal").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("openVal", e.getMessage());
            }
        }
        try {
            bond.setCloseVal(decimalFormat.parse(fieldSet.readRawString("closeVal").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("closeVal", e.getMessage());
            }
        }
        try {
            bond.setTrendWap(decimalFormat.parse(fieldSet.readRawString("trendWap").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("trendWap", e.getMessage());
            }
        }
        try {
            bond.setTrendClsPr(decimalFormat.parse(fieldSet.readRawString("trendClsPr").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("trendClsPr", e.getMessage());
            }
        }
        try {
            bond.setTrendWapPr(decimalFormat.parse(fieldSet.readRawString("trendWapPr").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("trendWapPr", e.getMessage());
            }
        }
        try {

            bond.setYieldAtWap(decimalFormat.parse(fieldSet.readRawString("yieldAtWap").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("yieldAtWap", e.getMessage());
            }
        }
        try {
            bond.setYieldClose(decimalFormat.parse(fieldSet.readRawString("yieldClose").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("yieldClose", e.getMessage());
            }
        }
        try {
            bond.setAccint(decimalFormat.parse(fieldSet.readRawString("accint").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("accint", e.getMessage());
            }
        }
        try {
            bond.setIricpiClose(decimalFormat.parse(fieldSet.readRawString("iricpiClose").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("iricpiClose", e.getMessage());
            }
        }
        try {
            bond.setBeiClose(decimalFormat.parse(fieldSet.readRawString("beiClose").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("beiClose", e.getMessage());
            }
        }
        try {
            bond.setCouponPercent(decimalFormat.parse(fieldSet.readRawString("couponPercent").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("couponPercent", e.getMessage());
            }
        }
        try {
            bond.setCouponValue(decimalFormat.parse(fieldSet.readRawString("couponValue").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("couponValue", e.getMessage());
            }
        }
        try {
            bond.setCbrClose(decimalFormat.parse(fieldSet.readRawString("cbrClose").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("cbrClose", e.getMessage());
            }
        }
        try {
            bond.setYieldToOffer(decimalFormat.parse(fieldSet.readRawString("yieldToOffer").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("yieldToOffer", e.getMessage());
            }
        }
        try {
            bond.setYieldLastCoupon(decimalFormat.parse(fieldSet.readRawString("yieldLastCoupon").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("yieldLastCoupon", e.getMessage());
            }
        }

        if (!errorModel.isEmpty()) {
            errors.put(bond.getIsin(), errorModel);
        }

        if (errorCollector.getFullErrorLogMapRPC().containsKey(bond.getTradeDate())) {
            if (!errorModel.isEmpty()) {
                errorCollector.getFullErrorLogMapRPC().get(bond.getTradeDate()).put(bond.getIsin(), errorModel);
            }
        } else {
            if (!errors.isEmpty()) {
                errorCollector.getFullErrorLogMapRPC().put(bond.getTradeDate(), errors);
            }
        }
        return bond;
    }
}