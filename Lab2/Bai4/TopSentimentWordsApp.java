import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;

public class TopSentimentWordsApp {

    public static class WordMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            // File output_cleaned có 5 cột: ID ; CleanedComment ; Category ; Aspect ; Sentiment
            if (parts.length >= 5) {
                String comment = parts[1].trim();
                String category = parts[2].trim();
                String sentiment = parts[4].trim().toLowerCase();
                
                if (!category.isEmpty() && !comment.isEmpty() && (sentiment.equals("positive") || sentiment.equals("negative"))) {
                    // Tạo Key ghép: Ví dụ "GENERAL [POSITIVE]"
                    String outKey = "[Phân loại: " + category + "] - Cảm xúc " + sentiment.toUpperCase();
                    String[] words = comment.split("\\s+");
                    for (String w : words) {
                        if (!w.isEmpty()) {
                            context.write(new Text(outKey), new Text(w));
                        }
                    }
                }
            }
        }
    }

    public static class WordReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> wordCount = new HashMap<>();
            
            // Đếm tần suất từng từ
            for (Text val : values) {
                String w = val.toString();
                wordCount.put(w, wordCount.getOrDefault(w, 0) + 1);
            }
            
            // Sắp xếp Map giảm dần theo Value (số lần xuất hiện)
            List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCount.entrySet());
            list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
            
            // Lấy Top 5
            StringBuilder top5 = new StringBuilder();
            int limit = Math.min(5, list.size());
            for (int i = 0; i < limit; i++) {
                top5.append(list.get(i).getKey()).append("(").append(list.get(i).getValue()).append(")");
                if (i < limit - 1) top5.append(", ");
            }
            
            context.write(key, new Text("-> Top 5 từ: " + top5.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 4: Top 5 Words by Category and Sentiment");
        job.setJarByClass(TopSentimentWordsApp.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Đầu vào bắt buộc phải là output_cleaned từ Bài 1
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
