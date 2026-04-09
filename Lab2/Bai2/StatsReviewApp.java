import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class StatsReviewApp {

    public static class StatsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length >= 5) {
                String comment = parts[1].trim();
                String category = parts[2].trim();
                String aspect = parts[3].trim();

                // 1. Thống kê từ (chỉ đếm các từ trong bình luận đã được làm sạch)
                String[] words = comment.split("\\s+");
                for (String w : words) {
                    if (!w.isEmpty()) {
                        outKey.set("WORD_" + w);
                        context.write(outKey, one);
                    }
                }

                // 2. Thống kê theo Category
                if (!category.isEmpty()) {
                    outKey.set("CAT_" + category);
                    context.write(outKey, one);
                }

                // 3. Thống kê theo Aspect
                if (!aspect.isEmpty()) {
                    outKey.set("ASP_" + aspect);
                    context.write(outKey, one);
                }
            }
        }
    }

    public static class StatsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            String k = key.toString();
            // Tách kết quả và áp dụng điều kiện > 500 cho các từ
            if (k.startsWith("WORD_")) {
                if (sum > 500) {
                    context.write(new Text("[Từ khóa > 500 lần] " + k.substring(5)), new IntWritable(sum));
                }
            } else if (k.startsWith("CAT_")) {
                context.write(new Text("[Phân loại] " + k.substring(4)), new IntWritable(sum));
            } else if (k.startsWith("ASP_")) {
                context.write(new Text("[Khía cạnh] " + k.substring(4)), new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 2: Review Statistics");
        job.setJarByClass(StatsReviewApp.class);
        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Chú ý: Đầu vào của Bài 2 chính là đầu ra đã làm sạch của Bài 1
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
