package RailwayFind.Model;



import RailwayFind.Utils.DateUtils;

import java.io.Serializable;

public class Record implements Serializable {
    private String GMSFHM;
    private String rowKey;
    private String SFD;

    @Override
    public String toString() {
        return "Record{" +
                "GMSFHM='" + GMSFHM + '\'' +
                ", rowKey='" + rowKey + '\'' +
                ", SFD='" + SFD + '\'' +
                ", MDD='" + MDD + '\'' +
                ", CC='" + CC + '\'' +
                ", CXH='" + CXH + '\'' +
                ", ZWH='" + ZWH + '\'' +
                ", FCSJ='" + FCSJ + '\'' +
                '}';
    }

    private String MDD;
    private String CC;
    private String CXH;
    private String ZWH;
    private String FCSJ;

    public Record(){

    }

    public String getFCSJ() {
        return FCSJ;
    }

    public void setFCSJ(String FCSJ) {
        this.FCSJ = FCSJ;
    }

    public Record( String rowKey,String GMSFHM, String SFD, String MDD, String CC, String CXH, String ZWH, String FCSJ) {
        this.GMSFHM = GMSFHM;
        this.rowKey = rowKey;
        this.SFD = SFD;
        this.MDD = MDD;
        this.CC = CC;
        this.CXH = CXH;
        this.ZWH = ZWH;
        this.FCSJ = FCSJ;
    }

    public String getGMSFHM() {
        return GMSFHM;
    }

    public void setGMSFHM(String GMSFHM) {
        this.GMSFHM = GMSFHM;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getSFD() {
        return SFD;
    }

    public void setSFD(String SFD) {
        this.SFD = SFD;
    }

    public String getMDD() {
        return MDD;
    }

    public void setMDD(String MDD) {
        this.MDD = MDD;
    }

    public String getCC() {
        return CC;
    }

    public void setCC(String CC) {
        this.CC = CC;
    }

    public String getCXH() {
        return CXH;
    }

    public void setCXH(String CXH) {
        this.CXH = CXH;
    }

    public String getZWH() {
        return ZWH;
    }

    public void setZWH(String ZWH) {
        this.ZWH = ZWH;
    }
}
