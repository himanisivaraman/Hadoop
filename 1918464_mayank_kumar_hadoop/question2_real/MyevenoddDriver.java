[cloudera@quikstart ~] $ hadoop jar EO.jar MyevenoddDriver .txt /output_MRDEMOB


// THIS IS THE  CODE OF reducer  even odd PROGRAM BY MAYANK KUMAR
Driver:

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyevenoddDriver 
{
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException 
	{

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Evenodd");  
		
		job.setJarByClass(MyevenoddDriver.class);
		job.setMapperClass(MyevenoddMapper.class);
		job.setReducerClass(MyevenoddReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


//THE CODE ENDS HERE 
