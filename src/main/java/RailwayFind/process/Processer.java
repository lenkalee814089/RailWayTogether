package RailwayFind.process;

import RailwayFind.Utils.FileReadWriteUtil;
import RailwayFind.process.utils.FindUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.io.IOException;
import java.util.*;
import RailwayFind.process.model.Record;
public class Processer {
    public static Dataset<Row> process(Dataset<Row> dataset, SparkSession sparkSession, int sameRailTimesHolder, int adjacentTimesHolder) throws IOException {

        JavaRDD<Row> rdd = dataset.javaRDD();

        JavaRDD<Row> rowJavaRDD = rdd.map(row -> {
            Record record = new Record(
                    row.getAs("GMSFHM"),
                    row.getAs("SFD"),
                    row.getAs("MDD"),
                    row.getAs("CC"),
                    row.getAs("CXH"),
                    row.getAs("ZWH"),
                    row.getAs("FCSJ")
            );

            System.out.println(record.toString());

            return record;

            //根据不同的证件号划分record
        }).mapToPair(record -> new Tuple2<>(record.getCC(), record))

                //让同车次的记录都到同一个区
                .partitionBy(new HashPartitioner(rdd.getNumPartitions()))

                //同个分区的record 进行分析处运算
                .mapPartitions(it -> {
                    Map<String, List<Record>> map = new HashMap<>();
                    List<Record> recordList;
                    Iterator iterator = null;

                    while (it.hasNext()) {
                        Record record = it.next()._2;
                        recordList = map.getOrDefault(record.getCC(), new LinkedList<Record>());
                        recordList.add(record);
                        map.put(record.getCC(), recordList);
                    }

                    List<Record> recordsOfCC;

                    List<Tuple2<String, int[]>> listOfCCRelations = new LinkedList<>();

                    Map<String, int[]> relationsInCC;
                    //遍历map的每个list，放入处理函数
                    for (Map.Entry<String, List<Record>> listEntry : map.entrySet()) {

                        recordsOfCC = listEntry.getValue();

                        relationsInCC = FindUtils.getResultMap(recordsOfCC);

                        relationsInCC.entrySet().forEach(x -> listOfCCRelations.add(new Tuple2<String, int[]>(x.getKey(), x.getValue())));

                    }
                    return listOfCCRelations.iterator();
                }).groupBy(tuple2 -> tuple2._1).map(x -> {//x:  key -> (),(),(),()....
                    String key = x._1;
                    String personA = key.split(":")[0];
                    String personB = key.split(":")[1];
                    int[] sumArr = {0, 0};
                    //相同两人关系的同铁路次数,邻座次数累加
                    x._2.forEach(tuple2 -> {
                        sumArr[0] += tuple2._2[0];
                        sumArr[1] += tuple2._2[1];
                    });
                    //字段说明: 人员A,人员B,同铁路次数,邻座次数
                    return RowFactory.create(personA, personB, sumArr[0]+"", sumArr[1]+"");
                }).filter(row -> Integer.parseInt(row.getString(2)) >= sameRailTimesHolder || Integer.parseInt(row.getString(3)) >= adjacentTimesHolder);


        String fildNames = "GMSFZH1,GMSFZH2,sameRailTimes,adjacentTimes";

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : fildNames.split(",")) {

            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> resultDataFrame = sparkSession.createDataFrame(rowJavaRDD, schema);

        return resultDataFrame ;
    }

}
