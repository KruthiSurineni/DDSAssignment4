
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Text;

public class EquiJoinClass {
	
	public static class Map extends Mapper<Text, Text, IntWritable, Text>{
		private Text row = new Text();
		private final static IntWritable joinColumn = new IntWritable();
		
		public void map(Text key, Text value, Context context)
		throwsIOException, iNterruptedException{
			
			String line = key.toString();
			String fields[] = line.split(",");
			String TableName = fields[0];
			int foo = Integer.parseInt(fields[1]);
			
			joinColumn.set(foo);
			row.set(line);
			context.write(joinColumn,row);
			
		}
		
	}
	
	
	public static class Reduce extends Reducer<IntWritable, Text, Text, Text>{
		
		public Text result = new Text();
		public void reduce(IntWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			List<String> Table1 = new ArrayList<String>();
			List<String> Table2 = new ArrayList<String>();
			String table1name = "";
			String table2name = "";
			
			String joinRows = new String();
			
			for (Text val : values) {
				String[] s = val.toString().split(",");
				if (s==null) {
					break;
				}
				if (table1name == "" ) {
					table1name = s[0];
					Table1.add(val.toString());
				}
				else if( table2name=="" ) {
					table2name = s[0];
				}
				else if( s[0].equals(table1name)) {
					Table1.add(val.toString());
				}
				else if( s[0].equals(table2name)) {
					Table2.add(val.toString());
				}
				
				}
			
			for (int i =0; i<Table1.size(); i++) {
				for (int j=0; j<Table2.size(); j++) {
					joinRows = Table1.get(i) + ", " + Table2.get(j);
					result.setData(joinRows);
					context.write("",result);
					Text result = new Text();
										
				}
				
				
			}
			
			
		}
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "Equi Join");
		 job.setJarByClass(EquiJoinClass.class);
		 job.setMapperClass(Map.class);
		 job.setReducerClass(Reduce.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
