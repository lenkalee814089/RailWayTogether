package RailwayFind.process.utils;


import org.apache.commons.lang.StringUtils;

public abstract   class StringUtil {
    /**
     * isEmpty?
     * @param s
     * @return
     */
    public static boolean isEmpty(String s){
        if (s == null || s.replace(" ", "").equals("")) {
            return true;
        }

        return false;
    }

    /**
     * 把两个旅馆编码连接，让某两个编码在组合时保证得到结果唯一
     * @param zjhm1
     * @param zjhm2
     * @return
     */
    public static String combine2GMSFZ(String GMSFZ1,String GMSFZ2){
        return GMSFZ1.hashCode() > GMSFZ2.hashCode() ? GMSFZ1+":" + GMSFZ2 : GMSFZ2+":" + GMSFZ1;
    }

    /**
     * 根据已知字符串生产定长字符串，长度不够左边补0
     * @param s
     * @param maxLength
     * @return
     */
    public static String getFixedLengthStr(String s, int maxLength)
    {

        s = StringUtil.isEmpty(s) ? "" : s.replace(" ", "");

        if (s.length() >= maxLength) {
            return s;
        }

        return generateZeroString(maxLength - s.length()) + s;

    }

    /**
     * 生成一个定长的纯0字符串
     *
     * @param length
     *          字符串长度
     * @return 纯0字符串
     */
    public static String generateZeroString(int length)
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            sb.append('0');
        }
        return sb.toString();
    }

    /**
     * 字符串转long
     * @param s
     * @return
     */
    public static long getLongFromString(String s)
    {
        long n = 0;

        try {
            n = StringUtils.isEmpty(s) ? 0 : Long.parseLong(s);
        } catch (Exception e) {
            n = 0;
        }

        return n;
    }

    /**
     * 字符串转long
     * @param o
     * @return
     */
    public static long getLongFromObject(Object o)
    {
        if (o == null || !(o instanceof Long)) {
            return 0;
        } else {
            return (Long) o;
        }
    }

    public static void main(String[] args)
    {
        System.out.println(getFixedLengthStr("123", 20));
    }
}