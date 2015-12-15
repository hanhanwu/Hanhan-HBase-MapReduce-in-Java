import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.codec.digest.DigestUtils;

public class LoadLogs extends Configured{
	static final Pattern patt 
	= Pattern.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$");

	static SimpleDateFormat dateparse = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ParseException {
		String tableName = args[0];
		String fileLocation = args[1];
		
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileLocation), "UTF8"))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       Put put = get_put(line);
		       put.add(Bytes.toBytes("raw"), Bytes.toBytes("line"), Bytes.toBytes(line));
		       table.put(put);
		    }
		}
		table.flushCommits();
		table.close();
	}
	
	@SuppressWarnings("deprecation")
	public static Put get_put (String line) throws ParseException, UnsupportedEncodingException {
		byte[] rowkey = DigestUtils.md5(line);
		Put put = new Put(Bytes.copy(rowkey));
		Matcher m = patt.matcher(line);
		while(m.find()) {
			String host = m.group(1);
			String dateStr = m.group(2);
			String path = m.group(3);
			String bytes = m.group(4);
			
			Date d = dateparse.parse(dateStr);
			Long date = d.getTime();
			
			put.add(Bytes.toBytes("struct"), Bytes.toBytes("host"), Bytes.toBytes(host));
			put.add(Bytes.toBytes("struct"), Bytes.toBytes("date"), Bytes.copy(date.toString().getBytes("UTF-8")));
			put.add(Bytes.toBytes("struct"), Bytes.toBytes("path"), Bytes.toBytes(path));
			put.add(Bytes.toBytes("struct"), Bytes.toBytes("bytes"), Bytes.toBytes(bytes));			
		}
		return put;
	}
}
