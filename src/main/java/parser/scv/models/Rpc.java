package parser.scv.models;

import lombok.Data;
import org.springframework.batch.item.ResourceAware;
import org.springframework.core.io.Resource;

import java.util.Date;

@Data
public class Rpc implements ResourceAware {
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
    private Float openPrice;
    private Float lowPrice;
    private Float highPrice;
    private Float closePrice;
    private Float waPrice;
    private Float trendClose;
    private Float openVal;
    private Float closeVal;
    private Float trendWap;
    private Float trendClsPr;
    private Float trendWapPr;
    private Float yieldAtWap;
    private Float yieldClose;
    private Float accint;
    private Long issueSize;
    private Integer decimals;
    private String type;
    private Float iricpiClose;
    private Float beiClose;
    private Float couponPercent;
    private Float couponValue;
    private Date buyBackDate;
    private Date lastTradeDate;
    private Float cbrClose;
    private Float yieldToOffer;
    private Float yieldLastCoupon;
    private Date offerDate;
    private String faceUnit;

    public Rpc() {
    }

    @Override
    public void setResource(Resource resource) {
        String temp = resource.toString();
        this.resource = temp.substring(temp.indexOf("mmvb", 0), temp.indexOf("]"));
    }
}
