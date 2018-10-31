package RailwayFind.DataMake;

import RailwayFind.process.utils.DateUtils;
import RailwayFind.process.utils.StringUtil;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 造数据
 */
public class DataProduce {
    private List<String> provinceList = new ArrayList<>();

    private List<String> trainNumList = new ArrayList<>();

    private String[] seatLetter = {"a","b","c","d","e","f"};

    public static Random random = new Random();
//"安徽省", "浙江省", "江苏省", "福建省", "广东省", "海南省", "四川省", "云南省", "贵州省", "青海省", " 甘肃省", "江西省", "台湾省"
//    "24000000D90E","1200000D100F","2400000D110Q","1200000D120L","2400000D130Q","1200000D1400","2400000D150V","1200000D160Q","2400000D170D",
//            "1200000D1808","2400000D190K","1100000D2008"
    public DataProduce(){

        String[] provinces = {"黑龙江省", "辽宁省", "吉林省", "河北省", "河南省", "湖北省", "湖南省", "山东省", "山西省", "陕西省",
                };
        String[] trainNum = {"24000000D10R","12000000D20H","24000000D30O","12000000D40E","24000000D50O","12000000D60G","24000000D70I","12000000D807",
                };

        for (String s : provinces) {
            provinceList.add(s);
        }
        for (String s : trainNum) {
            trainNumList.add(s);
        }

    }

    public static int randomInt(){
        return random.nextInt(9);
    }

    public static String getGMSFHM(int bits){
        String num="2";
        for (int i = 1; i < bits; i++) {

            num+=randomInt();

        }
        return num+"" ;
    }

    public Tuple2<String,String> getRanDateTuple(){
        String year = "2018";
        String month = "01";
        String day = "1"+randomInt();
        String hour = StringUtil.getFixedLengthStr(""+random.nextInt(1), 2) ;
        String min = StringUtil.getFixedLengthStr(""+random.nextInt(60), 2) ;
        String dateString1 = year + month + day + hour + min;
        long date2MillSeconds = DateUtils.parseYYYYMMDDHHMM2Date(dateString1).getTimeInMillis() + DateUtils.ONE_HOURS_SECONDS * 8;
        String dateString2 = DateUtils.getDateStringByMillisecond(DateUtils.HOUR_FORMAT, date2MillSeconds)+min;

        return new Tuple2<>(dateString1, dateString2) ;
    }

    /**
     * 始发地 目的地
     * @return
     */
    public Tuple2<String,String> getSFDMDD(){
        int index1 = random.nextInt(10);
        int index2 = random.nextInt(10);
        while (index1==index2){
            index2 = random.nextInt(10);
        }
        String province1 = provinceList.get(index1);
        String province2 = provinceList.get(index2);
        return new Tuple2<>(province1, province2);
    }

    public String getCC(){
        int index1 = random.nextInt(8);
        return trainNumList.get(index1);
    }

    public String getCXH(){
        return StringUtil.getFixedLengthStr(random.nextInt(3) + 1 + "", 2);
    }

    public String getZWH(){
        int index = random.nextInt(6);
        return StringUtil.getFixedLengthStr(random.nextInt(3) + 1 + "", 2)+seatLetter[index];
    }

    public static String getRzfh(){
        String rzfh = ""+random.nextInt(9)+ random.nextInt(1)+ random.nextInt(10);
        return rzfh;
    }

    public static String getLgbm(){
        String lgbm = ""+random.nextInt(1)+ random.nextInt(9)+ random.nextInt(9);
        return lgbm;
    }


}
