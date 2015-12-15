import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.codec.digest.DigestUtils;

public class LoadLogsMR extends Configured implements Tool {
	static final Pattern patt 
	= Pattern.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$");

	static SimpleDateFormat dateparse = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
	
	public static class LoadLogsReducer
	extends TableReducer<LongWritable, Text, LongWritable> {
		
		@SuppressWarnings("deprecation")
		@Override
        public void reduce(LongWritable key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
			for (Text val : values) {
				String line = val.toString();
				Put put;
				try {
					put = get_put(line);
					put.add(Bytes.toBytes("raw"), Bytes.toBytes("line"), Bytes.toBytes(line));
				    context.write(key, put);
				} catch (ParseException e) {
					e.printStackTrace();
				}		    
			}
        }
	}
	
	public static void main(String[] args) throws Exception {	
		int res = ToolRunner.run(new Configuration(), new LoadLogsMR(), args);
        System.exit(res);
	}
	
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "loadlogs mr");
        job.setJarByClass(LoadLogsMR.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableReducerJob(args[2], LoadLogsReducer.class, job);
        job.setNumReduceTasks(3);
       
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
 
        return job.waitForCompletion(true) ? 0 : 1;
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
