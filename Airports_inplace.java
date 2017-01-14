import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OperatingAirports {
	
	public static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
		
		Text v = new Text();
		
		public void map(LongWritable key, Text value, Context context)
		{
			String val = value.toString();
			
			if(val.length()>0) {
				
			String[] air=val.split(",");
			
			String ports=air[1];
			String country=air[3];
			
			if(country.equalsIgnoreCase("India")){
				v.set(ports);
				try {
					context.write(NullWritable.get(), v);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
				
				
			}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          
		Configuration conf= new Configuration();
		
		
		
		Job job = new Job(conf,"mywc");
		
		job.setJarByClass(OperatingAirports.class);
		
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);
		

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		
        //Configuring the input/output path from the filesystem into the job
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);		
		

	}


}
	}
