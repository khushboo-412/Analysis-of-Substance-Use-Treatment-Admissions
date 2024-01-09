import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class AgeYearCount {

    public static class AgeYearMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text ageYear = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 20 && isNumeric(fields[0]) && isNumeric(fields[20])) {
                int year = Integer.parseInt(fields[0]);
                int ageGroup = Integer.parseInt(fields[20]);
                ageYear.set(ageGroup + "\t" + year);
                context.write(ageYear, one);
            }
        }

        private boolean isNumeric(String str) {
            try {
                Integer.parseInt(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class HeaderTextOutputFormat extends TextOutputFormat<Text, IntWritable> {
        @Override
        public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Path file = getDefaultWorkFile(job, "");
            FileSystem fs = file.getFileSystem(job.getConfiguration());
            DataOutputStream fileOut = fs.create(file, false);
            fileOut.writeBytes("Age\tYear\tCount\n");
            return new LineRecordWriter<Text, IntWritable>(fileOut);
        }
    }

    public static class AgeYearPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.toString().hashCode()) % numPartitions;
        }
    }

    public static class AgeYearComparator extends WritableComparator {
        protected AgeYearComparator() {
            super(Text.class, true);
        }
    
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] partsA = a.toString().split("\t");
            String[] partsB = b.toString().split("\t");
    
            int ageA = Integer.parseInt(partsA[0]);
            int ageB = Integer.parseInt(partsB[0]);
            int ageComparison = Integer.compare(ageA, ageB);
    
            if (ageComparison == 0) {
                int yearA = Integer.parseInt(partsA[1]);
                int yearB = Integer.parseInt(partsB[1]);
                return Integer.compare(yearA, yearB);
            }
    
            return ageComparison;
        }
    }
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "age year count");
        job.setJarByClass(AgeYearCount.class);
        job.setMapperClass(AgeYearMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(HeaderTextOutputFormat.class);
        job.setPartitionerClass(AgeYearPartitioner.class);
        job.setSortComparatorClass(AgeYearComparator.class);
        job.setGroupingComparatorClass(AgeYearComparator.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
