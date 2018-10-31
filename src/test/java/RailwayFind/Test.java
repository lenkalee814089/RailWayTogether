package RailwayFind;

import scala.Tuple1;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class Test {
    public static void main(String[] args) {
        int [] a ={1};
        HashMap<Object,  int []> map = new HashMap<>();
        map.put(1,a );
        map.get(1)[0]++;
        System.out.println(map.get(1)[0]);

    }
}
