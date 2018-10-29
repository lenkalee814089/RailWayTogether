package RailwayFind.Utils;

import java.io.*;

public class FileReadWriteUtil {
    public static BufferedWriter getWriter(String path)throws IOException {
        File outFile = new File(path);
        if(!outFile.exists()){
            outFile.createNewFile();
        }
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile, true)));
        return out;
    }

    public static BufferedReader getReader(String path)throws IOException{
        String encoding = "utf-8";
        File file = new File(path);
        BufferedReader in =null;
        if (file.isFile() && file.exists()) { //判断文件是否存在
            InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);//考虑到编码格式
            in = new BufferedReader(read);
        }
        return in ;

    }

}
