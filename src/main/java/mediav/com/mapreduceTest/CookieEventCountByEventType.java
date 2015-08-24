package mediav.com.mapreduceTest;


import com.mediav.data.log.CookieEvent;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.hadoop.thrift.ParquetThriftInputFormat;

import java.io.IOException;




public class CookieEventCountByEventType extends Configured implements Tool {


	public enum EVENTTYPE {
		S, C, T, V, OTHER
	}

	public static class CookieEventMapper extends Mapper<Object, CookieEvent, Text, LongWritable> {

		private final static LongWritable one = new LongWritable(1);
		private Text eventType = new Text();
    

		@Override
    	protected void map(Object key, CookieEvent value, Context context) throws IOException, InterruptedException {
			int intEventType = value.getEventType();
			switch (intEventType) {
			case (int)'s':
				eventType.set(String.valueOf(EVENTTYPE.S));
				context.getCounter("input","EVENTTYPE.S").increment(1);
    			break;
			case (int)'c':
				eventType.set(String.valueOf(EVENTTYPE.C));	
				context.getCounter("input","EVENTTYPE.C").increment(1);
    			break;
			case (int)'t':
				eventType.set(String.valueOf(EVENTTYPE.T));
				context.getCounter("input","EVENTTYPE.T").increment(1);
    			break;
			case (int)'v':
				eventType.set(String.valueOf(EVENTTYPE.V));
				context.getCounter("input","EVENTTYPE.V").increment(1);
    			break;
			default:
				eventType.set(String.valueOf(EVENTTYPE.OTHER));
				context.getCounter("input","EVENTTYPE.OTHER").increment(1);
				break;    		    	    	
			}
			context.getCounter("input","ALL_EVENTTYPE").increment(1);
			context.write(eventType, one);
    }
  }

	public static class IntSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable result = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			
			for (LongWritable value:values) {    		
				sum += value.get();
			}    	
			
			result.set(sum);
			context.write(key, result);
		}
	}

	public int run(String[] args) throws Exception {

//		Configuration conf = getConf();
		Configuration conf = new Configuration();
/*		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  	System.err.println("Usage: CookieEventCountByEventType <in> <out>");
		  	System.exit(2);
		}*/
  
//		Path in = new Path(otherArgs[0]);
//		Path out = new Path(otherArgs[1]);
		Path in = new Path("/mvad/warehouse/session/dspan/date=2015-07-07");
		Path out = new Path("/user/shanjj/test/test4");
		
		// setup job
		Job job = Job.getInstance(conf);
		job.setJobName("[adhoc][CookieEventCountByEventType][20150824]");
		job.setJarByClass(CookieEventCountByEventType.class);
		job.setMapperClass(CookieEventMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// set InputFormatClass to be DelegateCombineFileInputFormat to Combine Small Splits
		job.setInputFormatClass(DelegateCombineFileInputFormat.class);
		DelegateCombineFileInputFormat.setCombinedInputFormatDelegate(job.getConfiguration(), ParquetThriftInputFormat.class);
		ParquetThriftInputFormat.addInputPath(job, in);

		// be sure to set ParquetThriftInputFormat ReadSupportClass and ThriftClass
		ParquetThriftInputFormat.setReadSupportClass(job, CookieEvent.class);
		ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), CookieEvent.class);    

		FileOutputFormat.setOutputPath(job, out);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new CookieEventCountByEventType(), args);
  }


}
