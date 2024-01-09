
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.io.Serializable;

public class SparkAgeYearCount {


    public static class TupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
            int ageCompare = t1._1.compareTo(t2._1);
            if (ageCompare != 0) {
                return ageCompare;
            } else {
                return t1._2.compareTo(t2._2);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: AgeYearCount <inputPath> <outputPath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf().setAppName("AgeYearCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> admissionsData = sc.textFile(inputPath);


        String firstLine = admissionsData.first();
        JavaRDD<String> filteredData = admissionsData.filter(line -> !line.equals(firstLine));

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> ageYearCounts = filteredData
                .filter(line -> {
                    String[] fields = line.split(",");
                    return fields.length > 20 && isNumeric(fields[0]) && isNumeric(fields[20]);
                })
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    int year = Integer.parseInt(fields[0].trim());
                    int ageGroup = Integer.parseInt(fields[20].trim());
                    Tuple2<Integer, Integer> ageYearKey = new Tuple2<>(ageGroup, year);
                    return new Tuple2<>(ageYearKey, 1);
                })
                .reduceByKey(Integer::sum);

        
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> sortedAgeYearCounts = ageYearCounts.sortByKey(new TupleComparator());

        
        JavaRDD<String> formattedOutput = sortedAgeYearCounts
                .map(tuple -> tuple._1._1 + "\t" + tuple._1._2 + "\t" + tuple._2);

        
        JavaRDD<String> header = sc.parallelize(Arrays.asList("Age\tYear\tCount"));
        JavaRDD<String> outputWithHeader = header.union(formattedOutput);

        
        outputWithHeader.coalesce(1).saveAsTextFile(outputPath);

        sc.stop();
    }

    private static boolean isNumeric(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
