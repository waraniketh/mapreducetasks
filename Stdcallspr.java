import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
public class Stdcallspr {
	public static class Stdmapper
    extends Mapper<Object, Text, Text, LongWritable>{
 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   String[] itr = value.toString().split(",");
   long start=toMillis(itr[2]);long stop=toMillis(itr[3]);
  if(Integer.parseInt(itr[4])==1){
   context.write(new Text(itr[0]),new LongWritable(stop-start)); 
  }}
private long toMillis(String date) {
     SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     Date dateFrm = null;
     try {
         dateFrm = format.parse(date);

     } catch (ParseException e) {

         e.printStackTrace();
    }
     return dateFrm.getTime();
 }   
}
public static class Stdreducer
    extends Reducer<Text,LongWritable,Text,LongWritable> {
 public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
   long total=0;
	 for (LongWritable val : values) {
	total+=val.get();}
	 if(total>60*60*100){
	 context.write(key,new LongWritable(total));}}
	   }

public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "word count");
 job.setJarByClass(Stdcallspr.class);
 job.setMapperClass(Stdmapper.class);
 job.setReducerClass(Stdreducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(LongWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
}}
