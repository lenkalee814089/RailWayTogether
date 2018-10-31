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
    public static Dataset<Row> process(Dataset<Row> dataset, SparkSession sparkSession, String columFamily, int parallelism) throws IOException {

        JavaRDD<Row> rdd = dataset.javaRDD();

        JavaRDD<Map<String,int[]>> JavaRDD
                = rdd.map(row -> {
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

            //根据不同的证件号划分ｒｅｃｏｒｄ
        }).mapToPair(record -> new Tuple2<>(record.getCC(), record))

                //让同车次的记录都到同一个区
                .partitionBy(new HashPartitioner(parallelism))

                //同个分区的record 进行分析处运算
                .mapPartitions(it -> {
                    Map<String, List<Record>> map = new HashMap<>();
                    List<Record> recordList ;
                    Iterator iterator = null;

                    while (it.hasNext()) {
                        Record record = it.next()._2;
                        recordList = map.getOrDefault(record.getCC(), new LinkedList<Record>());
                        recordList.add(record);
                        map.put(record.getCC(), recordList);
                    }

                    List<Record> recordsOfCC ;

                    List<Map<String,int[]>> resultOfPartion = new LinkedList<>();

                    //遍历map的每个list，放入处理函数
                    for (Map.Entry<String, List<Record>> listEntry : map.entrySet()) {

                        recordsOfCC = listEntry.getValue();

                        resultOfPartion.add(FindUtils.getResultMap(recordsOfCC));

                    }
                    return resultOfPartion.iterator();
                });

        //把每个partition的计算结果汇总
        List<Map<String, int[]>> collect = JavaRDD.collect();

        //汇总同行次数和邻座次数
        HashMap<String, int[]> sumMap = new HashMap<>();
        for (Map<String, int[]> map : collect) {

            String key;
            int[] countArr;
            int[] sumArr;

            for (Map.Entry<String, int[]> KV : map.entrySet()) {

                key = KV.getKey();

                countArr =KV.getValue();

                sumArr = sumMap.getOrDefault(key,new int[]{0,0});

                sumArr[0]+=countArr[0];
                sumArr[1]+=countArr[1];

                sumMap.put(key, sumArr);

            }
        }

        //结果放入两个list为后续写文件
        LinkedList<String> outPutList1 = new LinkedList<>();
        LinkedList<String> outPutList2 = new LinkedList<>();

        sumMap.entrySet().forEach(x -> {
            if(x.getValue()[0]>=2){
                outPutList1.add(x.getKey() +"的同行次数："+x.getValue()[0]);
                System.out.println(x.getKey() +"的同行次数："+x.getValue()[0]);
            }
            if(x.getValue()[1]>=2){
                outPutList2.add(x.getKey() +"邻座次数："+x.getValue()[1]);
                System.out.println(x.getKey() +"邻座次数："+x.getValue()[1]);
            }
        });


        //结果排序
        Collections.sort(outPutList1,FindUtils.getComparetor());
        Collections.sort(outPutList2, FindUtils.getComparetor());
        //写文件
        FileReadWriteUtil.write2File(outPutList1, "/home/lee/app/idea-IU-182.4892.20/workSop/RailwayFind/textFIle/铁路同行结果");
        FileReadWriteUtil.write2File(outPutList2, "/home/lee/app/idea-IU-182.4892.20/workSop/RailwayFind/textFIle/铁路邻座位结果");

        //结果转化为rdd

        LinkedList<String[]> resultList = new LinkedList<>();

        sumMap.entrySet().forEach(x -> {
            resultList.add(new String[]{x.getKey().split(":")[0],x.getKey().split(":")[1],x.getValue()[0]+"",x.getValue()[1]+""});
        });

        JavaRDD<String[]> resultRDD = new JavaSparkContext(sparkSession.sparkContext()).parallelize(resultList, parallelism);

        String fildNames = "GMSFZH1,GMSFZH2,sameTripTimes,adjacentTimes";

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : fildNames.split(",")) {

            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = resultRDD.map(arr -> RowFactory.create(arr[0], arr[1], arr[2], arr[3]));

        Dataset<Row> resultDataFrame = sparkSession.createDataFrame(rowJavaRDD, schema);

        return resultDataFrame ;
    }

}
