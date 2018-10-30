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

import java.io.IOException;
import java.util.*;


public  class Starter {

    private static final Logger LOGGER = Logger.getLogger(Starter.class);
    private JavaSparkContext jsc;
    private Configuration hConf;
    private HBaseAdmin admin;
    private SparkConf sparkConf;

    public Starter (SparkConf sparkConf1,String hbaseZkQuorum,String hbaseZkClientPort,String tablename) throws IOException {

        this.sparkConf=sparkConf1;

        this.jsc = new JavaSparkContext(sparkConf);

        this.hConf = HBaseConfiguration.create();

        //设置zooKeeper集群地址
        this.hConf.set("hbase.zookeeper.quorum",hbaseZkQuorum);

        //设置zookeeper连接端口，默认2181
        this.hConf.set("hbase.zookeeper.property.clientPort", hbaseZkClientPort);

        this.hConf.set(TableInputFormat.INPUT_TABLE, tablename);

        this.admin = new HBaseAdmin(hConf);

    }

    public JavaRDD<String[]> start(String tablename, String columFamily, SparkConf sparkConf, String hbaseZkQuorum, String hbaseZkClientPort,
                                   String sleepResultPath, String similarResultPath) throws IOException {

        if (!admin.isTableAvailable(tablename)) {
            LOGGER.error(tablename+"表不存在!");
            return null;
        }

        //读取数据并转化成rdd
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD
                = jsc.newAPIHadoopRDD(hConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD<String[]> resultRdd = process(hBaseRDD, columFamily, similarResultPath, sleepResultPath, 3);

        return resultRdd;

    }

    public JavaRDD<String[]> process(JavaPairRDD<ImmutableBytesWritable, Result> rdd, String columFamily, String togetherResultPath,
                                     String seatCLoseResultPath, int parallelism) throws IOException {
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
        FileReadWriteUtil.write2File(outPutList1, togetherResultPath);
        FileReadWriteUtil.write2File(outPutList2, seatCLoseResultPath);

        //结果转化为rdd
        LinkedList<String[]> resultList = new LinkedList<>();
        sumMap.entrySet().forEach(x -> {
            resultList.add(new String[]{x.getKey(),x.getValue()[0]+"",x.getValue()[1]+""});
        });
        JavaRDD<String[]> resultRDD = jsc.parallelize(resultList, parallelism);

        return resultRDD ;
    }




    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("HBaseTest").setMaster("local[3]");

        //注意,table中数据字段需要至少包含zjhm,lgbm,rzsj,tfsj四个字段,rowkey为处理证件号码+旅馆编码+入住时间
        String tableName="b";

        //列簇
        String columFamily ="f";

        String hbaseZkQuorum = "127.0.0.1";

        String hbaseZkClientPort = "2181";

        //输出结果的路径
        String togetherResultPath = "/home/lee/app/idea-IU-182.4892.20/workSop/RailwayFind/textFIle/铁路同行结果";
        String seatCLoseResultPath ="/home/lee/app/idea-IU-182.4892.20/workSop/RailwayFind/textFIle/铁路邻座位结果";

        Starter starter = new Starter(sparkConf,hbaseZkQuorum,hbaseZkClientPort,tableName);

        JavaRDD<String[]> resultRdd = starter.start(tableName, columFamily, sparkConf, hbaseZkQuorum,
                hbaseZkClientPort, togetherResultPath, seatCLoseResultPath);

        starter.jsc.stop();
        starter.admin.close();

    }




}
