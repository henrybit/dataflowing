import java.text.ParseException;
import java.text.SimpleDateFormat;


public class Test {

	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub
		String stamp1 = "2014-01-01 13:14:04";
	    String format1 = "yyyy-MM-dd HH:mm:ss";
	    //stamp1 = "Apr 1 2014 13:14:04";
	    //format1 = "MMM dd yyyy HH:mm:ss";
	    SimpleDateFormat formater = new SimpleDateFormat(format1);
	    long time = formater.parse(stamp1).getTime();
	    System.out.println(time);
	}

}
