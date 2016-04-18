package multisteps;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepOne {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String lines = value.toString();
            String mapkey = new String();
            String mapvalue = new String();
        	String[] line = lines.split(" ");
        	if(line[3].equals("A")){
        		mapkey = line[1];
        		mapvalue = line[3]+"\t"+line[0]+"\t"+line[2];
        	}else{
        		mapkey = line[0];
        		mapvalue = line[3]+"\t"+line[1]+"\t"+line[2];
        	}
        	context.write(new Text(mapkey), new Text(mapvalue));
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	        Iterator ite = values.iterator();
	        List<String> arrayA = new ArrayList<String>();
	        List<String> arrayB = new ArrayList<String>();
	        int v = 0;
	        while(ite.hasNext()){
	        	String record = ite.next().toString();
	        	String[] r = record.split("\t");
	        	v = Integer.parseInt(r[2]);
	        	if(r[0].equals("A"))
	        		arrayA.add(Integer.parseInt(r[1])+"\t"+v);
	        	if(r[0].equals("B"))
	        		arrayB.add(Integer.parseInt(r[1])+"\t"+v);
	        }
	    
	        String s_ret = "";
	        for(int m=0;m<arrayA.size();m++){
	        	String[] arrA = arrayA.get(m).split("\t");
	        	int i = Integer.parseInt(arrA[0]);
	        	int i_v = Integer.parseInt(arrA[1]);
	        	for(int n=0;n<arrayB.size();n++){		        	
		        	String[] arrB = arrayB.get(n).split("\t");		        	
		        	int k = Integer.parseInt(arrB[0]);
		        	int k_v = Integer.parseInt(arrB[1]);		        	
		        	s_ret += i + "," + k + "," + i_v * k_v + ";";		        	
	        	}
	        }	        
	        context.write(key, new Text(s_ret));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(new Configuration(), "StepOne");
		job.setJarByClass(StepOne.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])) ;
		System.exit(job.waitForCompletion(true ) ? 0 : 1);
	}
}
