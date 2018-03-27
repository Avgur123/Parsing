package parser.scv.mappers;

import parser.scv.FilesDataCollector;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;
import parser.scv.models.RepoCk;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class FieldSetMapperRepoCK implements FieldSetMapper {
    private FilesDataCollector errorCollector = FilesDataCollector.getCollector();

    private Map<String, Map<String, Object>> errors = new HashMap<>();

    private SimpleDateFormat parserFormat = new SimpleDateFormat("dd.MM.yyyy");
    private DecimalFormatSymbols decimalSymbols = new DecimalFormatSymbols();
    private DecimalFormat decimalFormat = new DecimalFormat("0.#");

    @Override
    public Object mapFieldSet(FieldSet fieldSet) throws BindException {
        RepoCk bond = new RepoCk();
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

        bond.setSecId(fieldSet.readRawString("secId"));
        bond.setBoardName(fieldSet.readRawString("boardName"));
        bond.setBoardId(fieldSet.readRawString("boardId"));
        bond.setShortName(fieldSet.readRawString("shortName"));
        bond.setRegNumber(fieldSet.readRawString("regNumber"));
        bond.setIsin(fieldSet.readRawString("isin"));
        bond.setCurrencyId(fieldSet.readRawString("currencyId"));
        bond.setType(fieldSet.readRawString("type"));

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

            bond.setDuration(fieldSet.readInt("duration"));
        } catch (NumberFormatException e) {
            if (!e.getMessage().equals("Unparseable number: ")) {
                errorModel.put("duration", e.getMessage());
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
            bond.setPrevLegalClosePrice(decimalFormat.parse(fieldSet.readRawString("prevLegalClosePrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("prevLegalClosePrice", e.getMessage());
            }
        }
        try {
            bond.setOpenPeriod(decimalFormat.parse(fieldSet.readRawString("openPeriod").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("openPeriod", e.getMessage());
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
            bond.setLegalOpenPrice(decimalFormat.parse(fieldSet.readRawString("legalOpenPrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("legalOpenPrice", e.getMessage());
            }
        }
        try {
            bond.setLow(decimalFormat.parse(fieldSet.readRawString("low").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("low", e.getMessage());
            }
        }
        try {
            bond.setHigh(decimalFormat.parse(fieldSet.readRawString("high").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("high", e.getMessage());
            }
        }
        try {
            bond.setLegalClosePrice(decimalFormat.parse(fieldSet.readRawString("legalClosePrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("legalClosePrice", e.getMessage());
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
            bond.setClosePeriod(decimalFormat.parse(fieldSet.readRawString("closePeriod").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("closePeriod", e.getMessage());
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
            bond.setTrendClsPr(decimalFormat.parse(fieldSet.readRawString("trendClsPr").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("trendClsPr", e.getMessage());
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
            bond.setTrendWapPr(decimalFormat.parse(fieldSet.readRawString("trendWapPr").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("trendWapPr", e.getMessage());
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
            bond.setHighBid(decimalFormat.parse(fieldSet.readRawString("highBid").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("highBid", e.getMessage());
            }
        }
        try {
            bond.setLowOffer(decimalFormat.parse(fieldSet.readRawString("lowOffer").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("lowOffer", e.getMessage());
            }
        }
        try {
            bond.setBid(decimalFormat.parse(fieldSet.readRawString("bid").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("bid", e.getMessage());
            }
        }
        try {
            bond.setOffer(decimalFormat.parse(fieldSet.readRawString("offer").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("offer", e.getMessage());
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
            bond.setMarketPrice(decimalFormat.parse(fieldSet.readRawString("marketPrice").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("marketPrice", e.getMessage());
            }
        }
        try {
            bond.setMarketPrice2(decimalFormat.parse(fieldSet.readRawString("marketPrice2").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("marketPrice2", e.getMessage());
            }
        }
        try {
            bond.setAdmittedQuote(decimalFormat.parse(fieldSet.readRawString("admittedQuote").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("admittedQuote", e.getMessage());
            }
        }
        try {
            bond.setMpvaltrd(decimalFormat.parse(fieldSet.readRawString("mpvaltrd").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("mpvaltrd", e.getMessage());
            }
        }
        try {
            bond.setMp2valtrd(decimalFormat.parse(fieldSet.readRawString("mp2valtrd").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("mp2valtrd", e.getMessage());
            }
        }
        try {
            bond.setAdmittedValue(decimalFormat.parse(fieldSet.readRawString("admittedValue").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("admittedValue", e.getMessage());
            }
        }
        try {
            bond.setMarketPrice3(decimalFormat.parse(fieldSet.readRawString("marketPrice3").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("marketPrice3", e.getMessage());
            }
        }
        try {
            bond.setMarketPrice3TradesValue(decimalFormat.parse(fieldSet.readRawString("marketPrice3TradesValue").replace(',', '.')).floatValue());
        } catch (ParseException e) {
            if (!e.getMessage().equals("Unparseable number: \"\"")) {
                errorModel.put("marketPrice3TradesValue", e.getMessage());
            }
        }

        if (!errorModel.isEmpty()) {
            errors.put(bond.getIsin(), errorModel);
        }

        if (errorCollector.getFullErrorLogMapRepoCK().containsKey(bond.getTradeDate())) {
            if (!errorModel.isEmpty()) {
                errorCollector.getFullErrorLogMapRepoCK().get(bond.getTradeDate()).put(bond.getIsin(), errorModel);
            }
        } else {
            if (!errors.isEmpty()) {
                errorCollector.getFullErrorLogMapRepoCK().put(bond.getTradeDate(), errors);
            }
        }
        return bond;
    }
}