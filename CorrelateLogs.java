import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class CorrelateLogs extends Configured implements Tool {
	
	public static class HBaseMapper
	extends TableMapper<Text, LongWritable> {  
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			Cell hostCell = value.getColumnLatestCell(Bytes.toBytes("struct"), Bytes.toBytes("host"));
			Cell bytesCell = value.getColumnLatestCell(Bytes.toBytes("struct"), Bytes.toBytes("bytes"));
			@SuppressWarnings("deprecation")
			byte[] hostVal = hostCell.getValue();
			String hostStr = new String(hostVal, "UTF-8");
			Text host = new Text(hostStr);
			@SuppressWarnings("deprecation")
			byte[] bytesVal = bytesCell.getValue();
			String bytesStr = new String(bytesVal, "UTF-8");
			LongWritable bytes = new LongWritable(Long.parseLong(bytesStr));
			context.write(host, bytes);
		}
	}
	
	public static class HBaseReducer
	extends Reducer<Text, LongWritable, Text, LongPairWritable> {
				
		@Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long requestCount = 0;
            long totalBytes = 0;           
            
            for (LongWritable val : values) {
                requestCount += 1;
                totalBytes += val.get();
            }
            LongPairWritable pair = new LongPairWritable (requestCount, totalBytes);
            context.write(key, pair);
        }
	}
	
	public static class AggregateMapper
	 extends Mapper<Text, LongPairWritable, Text, DoubleWritable>{
		protected static double n = 0;
		protected static double sumX, sumY = 0;
		protected static double sumXSqu, sumYSqu = 0;
		protected static double sumXY = 0;
		protected static double r, r2 = 0;
		
		
		@Override
        public void map(Text key, LongPairWritable value, Context context
                ) throws IOException, InterruptedException {
            long x = value.get_0();
            long y = value.get_1();
            
            n += 1;
            sumX += x; 
            sumY += y;
            sumXSqu += Math.pow(x, 2); 
            sumYSqu += Math.pow(y, 2);
            sumXY += x*y;
        }
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException { 
			double val1 = n*sumXY - sumX*sumY;
			double val2 = Math.sqrt((n*sumXSqu - Math.pow(sumX, 2)));
			double val3 = Math.sqrt((n*sumYSqu - Math.pow(sumY, 2)));
			r = val1/(val2*val3);
			r2 = Math.pow(r, 2);
			context.write(new Text("n"), new DoubleWritable(n));
			context.write(new Text("Sx"), new DoubleWritable(sumX));
			context.write(new Text("Sx2"), new DoubleWritable(sumXSqu));
			context.write(new Text("Sy"), new DoubleWritable(sumY));
			context.write(new Text("Sy2"), new DoubleWritable(sumYSqu));
			context.write(new Text("Sxy"), new DoubleWritable(sumXY));
			context.write(new Text("r"), new DoubleWritable(r));
			context.write(new Text("r2"), new DoubleWritable(r2));
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CorrelateLogs(), args);
        System.exit(res);
	}
	
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Configuration reduceConf = new Configuration(false);
        Configuration mapConf = new Configuration(false);
        Job job = Job.getInstance(conf, "correlate logs");
        job.setJarByClass(CorrelateLogs.class);    
                
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("struct"));
        TableMapReduceUtil.initTableMapperJob(args[0], scan, HBaseMapper.class, Text.class, LongWritable.class, job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);       
        
        job.setNumReduceTasks(1);
        
        ChainReducer.setReducer(job, HBaseReducer.class, Text.class, LongWritable.class,
        		Text.class, LongPairWritable.class, reduceConf);
        ChainReducer.addMapper(job, AggregateMapper.class, Text.class, LongPairWritable.class, Text.class, DoubleWritable.class, mapConf);
        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
