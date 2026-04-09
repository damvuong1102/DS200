import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;

public class TopCategoryWordsApp {

    public static class CategoryMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length >= 5) {
                String comment = parts[1].trim();
                String category = parts[2].trim();
                
                if (!category.isEmpty() && !comment.isEmpty()) {
                    String[] words = comment.split("\\s+");
                    for (String w : words) {
                        if (!w.isEmpty()) {
                            context.write(new Text("[Phân loại: " + category + "]"), new Text(w));
                        }
                    }
                }
            }
        }
    }

    public static class CategoryReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> wordCount = new HashMap<>();
            
            for (Text val : values) {
                String w = val.toString();
                wordCount.put(w, wordCount.getOrDefault(w, 0) + 1);
            }
            
            List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCount.entrySet());
            list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
            
            StringBuilder top5 = new StringBuilder();
            int limit = Math.min(5, list.size());
            for (int i = 0; i < limit; i++) {
                top5.append(list.get(i).getKey()).append("(").append(list.get(i).getValue()).append(")");
                if (i < limit - 1) top5.append(", ");
            }
            
            context.write(key, new Text("-> 5 từ liên quan nhất: " + top5.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 5: Top 5 Words by Category");
        job.setJarByClass(TopCategoryWordsApp.class);
        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CategoryReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
