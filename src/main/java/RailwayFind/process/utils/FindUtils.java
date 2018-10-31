package RailwayFind.process.utils;


import RailwayFind.process.model.Record;
import org.apache.log4j.Logger;

import java.util.*;

public class FindUtils {
    private static final Logger LOGGER = Logger.getLogger(FindUtils.class);

    /**
     * 判断两座位号是否邻座
     * @param a
     * @param b
     * @return
     */
    public static boolean isCloseSeatNUm(String a,String b){
        a=a.toLowerCase().substring(a.length()-1, a.length());
        b=b.toLowerCase().substring(b.length()-1, b.length());

        switch (a) {
            case "a":return b.equals("b") ;
            case "b":return b.equals("a")||b.equals("c");
            case "c":return b.equals("b") ;
            case "d":return b.equals("e") ;
            case "e":return b.equals("d")||b.equals("f");
            case "f":return b.equals("e") ;
        }
        LOGGER.error("处理是否邻座时出现错误!!");

        return false;
    }

    /**
     * 只判断两条记录座位是否相邻
     * @param recordA
     * @param recordB
     * @return
     */
    public static boolean isCloseSeat(Record recordA, Record recordB){
        if (recordA.getCXH().equals(recordB.getCXH())&&isCloseSeatNUm(recordA.getZWH(), recordB.getZWH()) ){
            return true;
        }
        return false;
    }

    /**
     * 判断是否同旅程,即,同出发时间,同始发地,同目的地,同车次
     * @param recordA
     * @param recordB
     * @return
     */
    public static boolean isSameRailway(Record recordA, Record recordB){
        return recordA.getCC().equals(recordB.getCC()) && recordA.getFCSJ().equals(recordB.getFCSJ())&&
                recordA.getMDD().equals(recordB.getMDD())&&recordA.getSFD().equals(recordB.getSFD());
    }
    /**
     * 获取任意两公民在某车次的同行和邻座
     * @param records
     * @return
     */
    public static Map<String,int[]> getResultMap(List<Record> records){
        Map<String,int[]> map = new HashMap<>();

        int[] arr ;
        String key;
        Record recordA ;
        Record recordB ;
        Iterator<Record> it2;
        Iterator<Record> it = records.iterator();

        while (it.hasNext()) {
            recordA=it.next();
            //把被判断的记录从集合移除，提高效率
            it.remove();
            it2 = records.iterator();
            while (it2.hasNext()) {
                recordB = it2.next();
                //同一人的记录不要进行判定
                if(!recordA.getGMSFHM().equals(recordB.getGMSFHM())){

                    //相似判断：如果某两条记录判定同行
                    if (isSameRailway(recordA, recordB)){

                        key = StringUtil.combine2GMSFZ(recordA.getGMSFHM()  ,recordB.getGMSFHM() );
                        //获得同行数和邻座数
                        arr = map.getOrDefault(key, new int[]{0, 0});
                        arr[0]++;
                        if (isCloseSeat(recordA, recordB)){

                            arr[1]++;
                        }
                        map.put(key, arr);
                    }
                }
            }
        }
        return map;
    }

    public static Comparator<String> getComparetor(){
        Comparator<String> comparator = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                String[] s1 = o1.split(":");
                String[] s2 = o2.split(":");
                return Integer.compare(Integer.parseInt(s1[0]), Integer.parseInt(s2[0]));
            }
        };
        return comparator;
    }

}
