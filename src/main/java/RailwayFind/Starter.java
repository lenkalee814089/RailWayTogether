package RailwayFind;

import RailwayFind.Model.Record;
import RailwayFind.Utils.FileReadWriteUtil;
import RailwayFind.Utils.FindUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public  class Starter {

    private static final Logger LOGGER = Logger.getLogger(Starter.class);
    private static JavaSparkContext jsc;
    private static   Configuration hConf;
    private static HBaseAdmin admin;
    private static SparkConf sparkConf;

    public void init (SparkConf sparkConf,String hbaseZkQuorum,String hbaseZkClientPort,String tablename) throws IOException {

        sparkConf=sparkConf;

        jsc = new JavaSparkContext(sparkConf);

        hConf = HBaseConfiguration.create();

        admin = new HBaseAdmin(hConf);

        //设置zooKeeper集群地址
        hConf.set("hbase.zookeeper.quorum",hbaseZkQuorum);

        //设置zookeeper连接端口，默认2181
        hConf.set("hbase.zookeeper.property.clientPort", hbaseZkClientPort);

        hConf.set(TableInputFormat.INPUT_TABLE, tablename);
    }

    public Tuple2<JavaRDD<String>, JavaRDD<String>> process(JavaPairRDD<ImmutableBytesWritable, Result> rdd, String columFamily, String similarResultPath,
                                                            String sleepResultPath, int parallelism) throws IOException {
        JavaRDD<Map<String,int[]>> JavaRDD
                = rdd.map(tuple -> {
            Result result = tuple._2;
            //获取行键
            //通过列族和列名获取列

            Record record = new Record(
                    Bytes.toString(result.getRow()),
                    Bytes.toString(result.getValue(columFamily.getBytes(), "GMSFHM".getBytes())),
                    Bytes.toString(result.getValue(columFamily.getBytes(), "SFD".getBytes())),
                    Bytes.toString(result.getValue(columFamily.getBytes(), "MDD".getBytes())),
                    Bytes.toString(result.getValue(columFamily.getBytes(), "CC".getBytes())),
                    Bytes.toString(result.getValue(columFamily.getBytes(), "CXH".getBytes())),
                    Bytes.toString(result.getValue(columFamily.getBytes(), "ZWH".getBytes())),
                    Bytes.toString(result.getValue(columFamily.getBytes(), "FCSJ".getBytes()))
            );

            return record;

            //根据不同的证件号划分ｒｅｃｏｒｄ
        }).mapToPair(record -> new Tuple2<String, Record>(record.getCC(), record))

                //让同车次的记录都到同一个区
                .partitionBy(new HashPartitioner(rdd.getNumPartitions()))

                //同个分区的record 进行分析处运算
                .mapPartitions(it -> {
                    Map<String, List<Record>> map = new HashMap<>();
                    List<Record> recordLinkedList = null;
                    Iterator iterator = null;

                    while (it.hasNext()) {
                        Record record = it.next()._2;
                        recordLinkedList = map.getOrDefault(record.getCC(), new LinkedList<Record>());
                        recordLinkedList.add(record);
                        map.put(record.getCC(), recordLinkedList);
                    }

                    List<Record> recordsOfHotel = null;

                    List<Map<String,int[]>> resultOfPartion = new LinkedList<>();

                    //遍历map的每个list，放入处理函数
                    for (Map.Entry<String, List<Record>> listEntry : map.entrySet()) {

                        recordsOfHotel = listEntry.getValue();



                        resultOfPartion.add(FindUtils.getResultMap(recordsOfHotel));

                    }

                    return resultOfPartion.iterator();

                });

        //把每个partition的计算结果汇总
        List<Map<String, int[]>> collect = JavaRDD.collect();



        //汇总同行次数和邻座次数
        HashMap<String, int[]> resultMap = new HashMap<>();
        for (Map<String, int[]> map : collect) {

            String key;
            int[] countArr;
            int[] sumArr;

            for (Map.Entry<String, int[]> KV : map.entrySet()) {

                key = KV.getKey();

                countArr =KV.getValue();

                sumArr = resultMap.getOrDefault(key,new int[]{0,0});

                sumArr[0]+=countArr[0];
                sumArr[1]+=countArr[1];

                resultMap.put(key, sumArr);
            }

        }

        BufferedWriter out1 = FileReadWriteUtil.getWriter(similarResultPath);
        LinkedList<String> resultList1 = new LinkedList<>();
        LinkedList<String> resultList2 = new LinkedList<>();

        resultMap.entrySet().forEach(x -> {
            if(x.getValue()[0]>=4){
                resultList1.add(x.getKey() +"的同行次数："+x.getValue());
            }
            if(x.getValue()[1]>=2){
                resultList2.add(x.getKey() +"邻座次数："+x.getValue());
            }
        });




        //结果排序
        Collections.sort(resultList1,FindUtils.getComparetor());
        Collections.sort(resultList2, FindUtils.getComparetor());

        JavaRDD<String> togetherRDD = jsc.parallelize(resultList1, parallelism);

        JavaRDD<String> closeSeatRDD = jsc.parallelize(resultList2, parallelism);

        return new Tuple2<>(togetherRDD,closeSeatRDD) ;

        //写到文件
//        write2File(resultList1, similarResultPath);
//        write2File(resultList2, sleepResultPath);
    }


    public Tuple2<JavaRDD<String>, JavaRDD<String>> start(String tablename,String columFamily,SparkConf sparkConf,String hbaseZkQuorum,String hbaseZkClientPort,
                                                          String sleepResultPath,String similarResultPath) throws IOException {
        //初始化
        init(sparkConf,hbaseZkQuorum,hbaseZkClientPort,tablename);


        if (!admin.isTableAvailable(tablename)) {
            LOGGER.error(tablename+"表不存在!");
            return null;
        }

        //读取数据并转化成rdd
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD
                = jsc.newAPIHadoopRDD(hConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        Tuple2<JavaRDD<String>, JavaRDD<String>> rddTuple2 = process(hBaseRDD, columFamily, similarResultPath, sleepResultPath, 3);

        return rddTuple2;

    }

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("HBaseTest").setMaster("local[3]");

        //注意,table中数据字段需要至少包含zjhm,lgbm,rzsj,tfsj四个字段,rowkey为处理证件号码+旅馆编码+入住时间
        String tableName="a";

        //列簇
        String columFamily ="f";

        String HbaseZkQuorum = "127.0.0.1";

        String HbaseZkClientPort = "2181";

        //输出结果的路径
        String sleepResultPath = "/home/lee/app/idea-IU-182.4892.20/workSop/FindTogether/src/main/Text/同宿结果";
        String similarResultPath ="/home/lee/app/idea-IU-182.4892.20/workSop/FindTogether/src/main/Text/同行结果";

        Starter starter = new Starter();

        Tuple2<JavaRDD<String>, JavaRDD<String>> rddTuple2 = starter.start(tableName, columFamily, sparkConf, HbaseZkQuorum,
                HbaseZkClientPort, sleepResultPath, similarResultPath);

        jsc.stop();
        admin.close();

    }




}
