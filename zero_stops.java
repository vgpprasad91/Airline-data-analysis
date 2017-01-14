package airlines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AirlinesStops {
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String val = value.toString();
			
			if(val.length()>0) {
				
				String[] routes=val.split(",");
			
				String air = routes[0];
				String stops= routes[7];
				
				if(Integer.parseInt(stops)==0) {
					Text v = new Text();
					
					v.set(air);
					context.write(NullWritable.get(), v);
					
				}
			}
			
		}
		
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
Configuration conf= new Configuration();
		
		
		
		Job job = new Job(conf,"zeroStops");
		
		job.setJarByClass(AirlinesStops.class);
		
		job.setMapperClass(Map.class);
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
