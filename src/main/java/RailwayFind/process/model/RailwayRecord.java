package RailwayFind.process.model;


import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public class RailwayRecord implements Serializable {
    private String GMSFHM;
    private String SFD;
    private String MDD;
    private String CC;
    private String CXH;
    private String ZWH;
    private String FCSJ;
    private JSONObject diffFieldJsonObject;
    private JSONObject sameFieldJsonObject;


    public JSONObject getDiffFieldJsonObject() {
        return diffFieldJsonObject;
    }

    public void setDiffFieldJsonObject(JSONObject diffFieldJsonObject) {
        this.diffFieldJsonObject = diffFieldJsonObject;
    }

    public JSONObject getSameFieldJsonObject() {
        return sameFieldJsonObject;
    }

    public void setSameFieldJsonObject(JSONObject sameFieldJsonObject) {
        this.sameFieldJsonObject = sameFieldJsonObject;
    }

    public RailwayRecord(){

    }

    public String getFCSJ() {
        return FCSJ;
    }

    public void setFCSJ(String FCSJ) {
        this.FCSJ = FCSJ;
    }

    public RailwayRecord(String GMSFHM, String SFD, String MDD, String CC, String CXH, String ZWH, String FCSJ, JSONObject diffFieldJsonObject, JSONObject sameFieldJsonObject) {
        this.GMSFHM = GMSFHM;
        this.SFD = SFD;
        this.MDD = MDD;
        this.CC = CC;
        this.CXH = CXH;
        this.ZWH = ZWH;
        this.FCSJ = FCSJ;
        this.diffFieldJsonObject = diffFieldJsonObject;
        this.sameFieldJsonObject = sameFieldJsonObject;
    }

    public String getGMSFHM() {
        return GMSFHM;
    }

    public void setGMSFHM(String GMSFHM) {
        this.GMSFHM = GMSFHM;
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
