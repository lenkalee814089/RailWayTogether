package RailwayFind.HbaseHandle;


import RailwayFind.Utils.FileReadWriteUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

public class Processer {


    public static void write2File(List list,String path) throws IOException {

        BufferedWriter out = FileReadWriteUtil.getWriter(path);

        list.forEach(x -> {
            try {
                out.write(x+"\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        out.close();
    }
}
