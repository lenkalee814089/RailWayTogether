package RailwayFind.process;

import RailwayFind.process.model.RailwayRecord;
import RailwayFind.process.utils.RailwayFindUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;
public class Processer {
    /**
     * 铁路同行
     * 同行规则：同铁路班次（发车时间相同,出发点,目的地,车次相同），同行次数加一
     * 同铁路次数不小于4的为同行人
     * 邻座次数不小于2次的为同行人
     * @param df 数据集
     * @param sameRailTimesThreshold 同铁路同行次数阈值
     * @param adjacentTimesThreshold 邻座次数阈值
     * @param GMSFHM 公民身份证号码
     * @param SFD 始发地
     * @param MDD 目的地
     * @param CC  车次
     * @param CXH  车厢号
     * @param ZWH 座位号
     * @param FCSJ 发车时间
     * @param railColums 火车信息列 以分号";"分隔
     * @returdn
     */
    public static Dataset<Row> railWayCompute(Dataset<Row> df,  int sameRailTimesThreshold, int adjacentTimesThreshold,String GMSFHM,String SFD ,
                                       String MDD,String CC,String CXH ,String ZWH,String FCSJ ,String railColums,String railwayCommonColums) {

        JavaRDD<Row> rdd = df.javaRDD();
        JavaRDD<Row> rowJavaRDD = rdd.filter(row -> row.getAs(GMSFHM) != null && row.getAs(SFD) != null && row.getAs(MDD) != null
                && row.getAs(CC) != null && row.getAs(CXH) != null && row.getAs(ZWH) != null && row.getAs(FCSJ) != null)
                .map(row -> {

                    JSONObject diffJsonObject = new JSONObject();
                    String[] railColumnsSPlit = railColums.split(";");
                    for (String colum : railColumnsSPlit) {
                        diffJsonObject.put(colum, row.getAs(colum));
                    }
                    JSONObject commonJsonObject  = new JSONObject();
                    String[] commonColumnsSPlit = railwayCommonColums.split(";");
                    for (String colum : commonColumnsSPlit) {
                        commonJsonObject.put(colum, row.getAs(colum));
                    }

                    RailwayRecord record = new RailwayRecord(
                            row.getAs(GMSFHM),
                            row.getAs(SFD),
                            row.getAs(MDD),
                            row.getAs(CC),
                            row.getAs(CXH),
                            row.getAs(ZWH),
                            row.getAs(FCSJ),
                            diffJsonObject,
                            commonJsonObject
                    );

                    System.out.println(record.toString());

                    return record;

                    //根据不同的证件号划分record
                }).mapToPair(record -> new Tuple2<>(record.getCC(), record))

                //让同车次的记录都到同一个区
                .partitionBy(new HashPartitioner(rdd.getNumPartitions()))

                //同个分区的record 进行分析处运算
                .mapPartitions(it -> {
                    Map<String, List<RailwayRecord>> map = new HashMap<>();
                    List<RailwayRecord> recordList;
                    Iterator iterator = null;

                    while (it.hasNext()) {
                        RailwayRecord record = it.next()._2;
                        recordList = map.getOrDefault(record.getCC(), new LinkedList<RailwayRecord>());
                        recordList.add(record);
                        map.put(record.getCC(), recordList);
                    }

                    List<RailwayRecord> recordsOfCC;

                    //tuple(a:b, 存放a,b同行和邻座信息的list数组)
                    List<Tuple2<String, List<JSONObject>[]>> listOfCCRelations = new LinkedList<>();

                    Map<String, List<JSONObject>[]> relationsInCC;
                    //遍历map的每个list，放入处理函数
                    for (Map.Entry<String, List<RailwayRecord>> listEntry : map.entrySet()) {

                        recordsOfCC = listEntry.getValue();

                        relationsInCC = RailwayFindUtils.getResultMap(recordsOfCC);

                        relationsInCC.entrySet().forEach(x -> listOfCCRelations.add(new Tuple2<String, List<JSONObject>[]>(x.getKey(), x.getValue())));

                    }
                    return listOfCCRelations.iterator();
                }).groupBy(tuple2 -> tuple2._1).map(x -> {//x:  key -> {list,list}....
                    String key = x._1;
                    String personA = key.split(":")[0];
                    String personB = key.split(":")[1];
                    //将所有a:b的同行记录合并
                    List<JSONObject>[] sumArr = new List[]{new LinkedList<>(), new LinkedList<>(),new LinkedList<>(), new LinkedList<>()};
                    //相同两人关系的同铁路次数,邻座次数累加
                    x._2.forEach(tuple2 -> {
                        sumArr[0].addAll(tuple2._2[0]);
                        sumArr[1].addAll(tuple2._2[1]);
                        sumArr[2].addAll(tuple2._2[2]);
                        sumArr[3].addAll(tuple2._2[3]);
                    });
                    //用","连接
                    String sameRailInfo = "";
                    String adjacentInfo = "";
                    String sameRailCommonInfo ="";
                    String adjacentCommonInfo ="";
                    for (JSONObject jsonObject : sumArr[0]) {
                        sameRailInfo += jsonObject.toJSONString()+",";
                    }
                    for (JSONObject jsonObject : sumArr[1]) {
                        adjacentInfo += jsonObject.toJSONString()+",";
                    }
                    for (JSONObject jsonObject : sumArr[2]) {
                        sameRailCommonInfo+=jsonObject.toJSONString()+",";
                    }
                    for (JSONObject jsonObject : sumArr[3]) {
                        adjacentCommonInfo+=jsonObject.toJSONString()+",";
                    }
                    //去除最后一个逗号
                    if (sameRailInfo.length()!=0){
                        sameRailInfo=sameRailInfo.substring(0,sameRailInfo.length()-1);
                    }
                    if (adjacentInfo.length()!=0){
                        adjacentInfo=adjacentInfo.substring(0,adjacentInfo.length()-1);
                    }
                    if (sameRailCommonInfo.length()!=0){
                        sameRailCommonInfo = sameRailCommonInfo.substring(0, sameRailCommonInfo.length() - 1);
                    }
                    if (adjacentCommonInfo.length()!=0){
                        adjacentCommonInfo = adjacentCommonInfo.substring(0, adjacentCommonInfo.length() - 1);
                    }


                    //字段说明: 人员A,人员B,同铁路次数,邻座次数,同铁路信息,邻座的信息
                    return RowFactory.create(personA, personB, sumArr[0].size()/2 + "", sumArr[1].size()/2 + "", sameRailInfo, adjacentInfo,sameRailCommonInfo,adjacentCommonInfo);
                }).filter(row -> Integer.parseInt(row.getString(2)) >= sameRailTimesThreshold || Integer.parseInt(row.getString(3)) >= adjacentTimesThreshold);


        String fildNames = "GMSFZH1,GMSFZH2,sameRailTimes,adjacentTimes,sameRailInfo,adjacentInfo,sameRailCommonInfo,adjacentCommonInfo";

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : fildNames.split(",")) {

            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> resultDataFrame = df.sparkSession().createDataFrame(rowJavaRDD, schema);

        return resultDataFrame ;
    }

}
