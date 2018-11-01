package RailwayFind.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public  class Starter {

    private static final Logger LOGGER = Logger.getLogger(Starter.class);
    private static JavaSparkContext jsc;
    private static Configuration hConf;
    private static HBaseAdmin admin;
    private static SparkConf sparkConf;

    public static void init (SparkConf sparkConf1,String hbaseZkQuorum,String hbaseZkClientPort,String tablename) throws IOException {

        sparkConf=sparkConf1;

        jsc = new JavaSparkContext(sparkConf);

        hConf = HBaseConfiguration.create();
        hConf.set("hbase.zookeeper.quorum",hbaseZkQuorum);
        hConf.set("hbase.zookeeper.property.clientPort", hbaseZkClientPort);
        hConf.set(TableInputFormat.INPUT_TABLE, tablename);

        admin = new HBaseAdmin(hConf);
    }

    public static Dataset<Row> start(String tablename, String columFamily, SparkConf sparkConf, String hbaseZkQuorum, String hbaseZkClientPort) throws IOException {

        init(sparkConf,hbaseZkQuorum,hbaseZkClientPort,tablename);

        //布置的测试环境----
        if (!admin.isTableAvailable(tablename)) {
            LOGGER.error(tablename+"表不存在!");
            return null;
        }

        //读取数据并转化成rdd
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD
                = jsc.newAPIHadoopRDD(hConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        //转df
        String fildNames = "GMSFHM,SFD,MDD,CC,CXH,ZWH,FCSJ";

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : fildNames.split(",")) {

            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = hBaseRDD.map(pair -> RowFactory.create(
                Bytes.toString(pair._2.getValue(columFamily.getBytes(), "GMSFHM".getBytes())),
                Bytes.toString(pair._2.getValue(columFamily.getBytes(), "SFD".getBytes())),
                Bytes.toString(pair._2.getValue(columFamily.getBytes(), "MDD".getBytes())),
                Bytes.toString(pair._2.getValue(columFamily.getBytes(), "CC".getBytes())),
                Bytes.toString(pair._2.getValue(columFamily.getBytes(), "CXH".getBytes())),
                Bytes.toString(pair._2.getValue(columFamily.getBytes(), "ZWH".getBytes())),
                Bytes.toString(pair._2.getValue(columFamily.getBytes(), "FCSJ".getBytes()))
        ));

        SparkSession sparkSession = new SparkSession(jsc.sc());

        Dataset<Row> hbaseDf = sparkSession.createDataFrame(rowJavaRDD, schema);
    //----布置的测试环境

    String railColumn="GMSFHM;CXH;ZWH";
    String commonColuns ="SFD;MDD;CC;FCSJ";
    //---------insight组件-------------------------------------------------------------------------------------------------------------------------------------
        Dataset<Row> resultDf = Processer.railWayCompute(hbaseDf, 4,  1,"GMSFHM","SFD","MDD","CC",
                "CXH","ZWH","FCSJ",railColumn,commonColuns);
    //----------------------------------------------------------------------------------------------------------------------------------------------

        return resultDf;

    }



    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("HBaseTest").setMaster("local[3]");

        //注意,table中数据字段需要至少包含GMSFHM,SFD,MDD,CC,CXH,ZWH,FCSJ 7个字段
        String tableName="b";

        String columFamily ="f";

        String hbaseZkQuorum = "127.0.0.1";

        String hbaseZkClientPort = "2181";

        Dataset<Row> dataset = Starter.start(tableName, columFamily, sparkConf, hbaseZkQuorum, hbaseZkClientPort);

        dataset.foreach((ForeachFunction<Row>) row -> System.out.println(row.toString()));

        Starter.jsc.stop();
        Starter.admin.close();

    }




}
