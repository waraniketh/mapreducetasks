
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
public class verifymaletree {
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split("\t");
		context.write(new Text("waraniketh"),new Text(str[1]+","+str[0]));
	}
}
public static class Myreducer extends Reducer<Text,Text,Text,LongWritable>{
	     long[] order=new long[100];int i=0;String[] str2=new String[100];
	     TreeMap<Long,String> map1=new TreeMap<Long,String>();
		public void reduce(Text key1,Iterable<Text> value2,Context context) throws IOException, InterruptedException{
			for(Text value1:value2){
			String[] str1=value1.toString().split(",");
			long maleorder=Long.parseLong(str1[0]);
            map1.put(Long.parseLong(str1[0]),str1[1]);
			order[i]=maleorder;
            str2[i]=str1[1];i++;
			}
			Set<Long> set1=map1.keySet();int check=set1.size()-10;int how=0;
			Iterator<Long> itr=set1.iterator();
			while(itr.hasNext()){if(how>check){
				long lol=itr.next();
				context.write(new Text(map1.get(lol)),new LongWritable(lol));how++;
			}else{itr.next();how++;}}
		}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"myfirstpro");
		job.setJarByClass(verifymaletree.class);
		job.setMapperClass(MyMapper.class);
	    job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
	
	
}
