package parser.excel.models;

import lombok.Data;

import java.util.Date;

@Data
public class Director {
    private String emitentName;
    private Long inn;
    private Date date;
    private String status;
    private String managingAuthority;
    private String directorName;
    private Integer birthday;
    private String education;
    private String isChairman;
    private String periodFrom;
    private String periodTo;
    private String companyName;
    private String position;
    private Float capitalShare;
    private Float sequrityShare;
    private String relations;
    private String crime;
    private String crimeJob;

    public Director() {
    }
}
