package RailwayFind.HbaseHandle;

import RailwayFind.DataMake.DataProduce;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.LinkedList;

public class handle {
    /**
     * 向ｈｂａｓｅ插入数据
     * @throws IOException
     */
    public static void putsData() throws IOException {


        SparkContext sc = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("hbase"));
        JobConf jobConf =null;
        jobConf = new JobConf(HBaseConfiguration.create());
        jobConf.set("hbase.zookeeper.quorum", "127.0.0.1");
        // jobConf.set("zookeeper.znode.parent", "/hbase");
        jobConf.setOutputFormat(TableOutputFormat.class);
        HTable table = new HTable(jobConf, "b");


        LinkedList<Put> puts = new LinkedList<>();
        DataProduce dataProducer = new DataProduce();
        Put put=null;
        for (int i = 0; i < 20000; i++) {
            Tuple2<String, String> tuple = dataProducer.getSFDMDD();
            Tuple2<String, String> dataTuple = dataProducer.getRanDateTuple();

            String GMSFHM = DataProduce.getGMSFHM(3);
            String SFD = tuple._1;
            String MDD =tuple._2;
            String CC = dataProducer.getCC();
            String CXH = dataProducer.getCXH();
            String ZWH = dataProducer.getZWH();
            String FCSJ = dataTuple._1;
            String rowKey =GMSFHM+CC+FCSJ;

            //rowKey
            put = new Put(Bytes.toBytes(rowKey)) ;
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("GMSFHM"), Bytes.toBytes(GMSFHM));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("SFD"), Bytes.toBytes(SFD));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("MDD"), Bytes.toBytes(MDD));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("CC"), Bytes.toBytes(CC));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("CXH"), Bytes.toBytes(CXH));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("ZWH"), Bytes.toBytes(ZWH));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("FCSJ"), Bytes.toBytes(FCSJ));
            puts.add(put);
        }

        table.put(puts);
    }

    public static void main(String[] args) throws IOException {
        putsData();

        
    }
}
