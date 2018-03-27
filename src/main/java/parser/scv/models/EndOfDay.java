package parser.scv.models;

import lombok.Data;
import org.springframework.batch.item.ResourceAware;
import org.springframework.core.io.Resource;

import java.util.Date;

@Data
public class EndOfDay implements ResourceAware {
    private String resource;
    private String fileName;
    private Date tradeDate;
    private String secId;
    private String boardName;
    private String boardId;
    private String shortName;
    private String regNumber;
    private String isin;
    private Date matDate;
    private Integer listName;
    private Float faceValue;
    private String currencyId;
    private Integer volume;
    private Float value;
    private Integer numTrades;
    private Float prevPrice;
    private Float prevLegalClosePrice;
    private Float openPeriod;
    private Float openPrice;
    private Float legalOpenPrice;
    private Float low;
    private Float high;
    private Float legalClosePrice;
    private Float closePrice;
    private Float closePeriod;
    private Float waPrice;
    private Float trendClose;
    private Float trendClsPr;
    private Float trendWap;
    private Float trendWapPr;
    private Float openVal;
    private Float closeVal;
    private Float highBid;
    private Float lowOffer;
    private Float bid;
    private Float offer;
    private Float yieldAtWap;
    private Float yieldClose;
    private Float accint;
    private Integer duration;
    private Float marketPrice;
    private Float marketPrice2;
    private Float admittedQuote;
    private Long issueSize;
    private Float mpvaltrd;
    private Float mp2valtrd;
    private Float admittedValue;
    private Float marketPrice3;
    private Float marketPrice3TradesValue;
    private Integer decimals;
    private String type;

    public EndOfDay() {
    }

    @Override
    public void setResource(Resource resource) {
        String temp = resource.toString();
        this.resource = temp.substring(temp.indexOf("mmvb", 0), temp.indexOf("]"));
    }
}
