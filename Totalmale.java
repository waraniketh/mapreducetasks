import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Totalmale {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split(",");
			context.write(new Text("aniketh"),new Text(str[4]));
		}
	}
	public static class Myreducer extends Reducer<Text,Text,Text,LongWritable>{
		
		public void reduce(LongWritable key1,Iterable<Text> value1,Context context) throws IOException, InterruptedException{
			for(Text tet:value1){long lol=0;
				lol=lol+Long.parseLong(tet.toString());
				context.write(new Text("total"), new LongWritable(lol));
			}
		}
	}
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"maletotal");
		job.setJarByClass(Totalmale.class);
		job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
	


}
