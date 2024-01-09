import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UniqueCBSA {

    public static class CBSAMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
        private IntWritable cbsaCode = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            try {
                int code = Integer.parseInt(fields[3].trim()); 
                cbsaCode.set(code);
                context.write(cbsaCode, NullWritable.get());
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class UniqueReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
        public void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UniqueCBSA <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Unique CBSA");
        job.setJarByClass(UniqueCBSA.class);
        job.setMapperClass(CBSAMapper.class);
        job.setCombinerClass(UniqueReducer.class); 
        job.setReducerClass(UniqueReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
