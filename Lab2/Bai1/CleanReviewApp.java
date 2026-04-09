import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.HashSet;

public class CleanReviewApp {

    public static class CleanMapper extends Mapper<Object, Text, NullWritable, Text> {
        private HashSet<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0]))));
                String word;
                while ((word = br.readLine()) != null) {
                    stopWords.add(word.trim().toLowerCase());
                }
                br.close();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Dùng dấu chấm phẩy dựa theo ảnh bạn chụp
            String[] columns = value.toString().split(";"); 
            
            // Đảm bảo dòng có đủ 5 cột
            if (columns.length >= 5) {
                String rawComment = columns[1]; // Cột số 1 là nội dung bình luận
                
                // 1. Đưa về chữ thường và 2. Bỏ ký tự đặc biệt
                String cleanText = rawComment.toLowerCase().replaceAll("[^a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ\\s]", "");
                
                // 3. Tách từ và loại stop word
                String[] words = cleanText.split("\\s+");
                StringBuilder finalComment = new StringBuilder();
                for (String w : words) {
                    if (!w.isEmpty() && !stopWords.contains(w)) {
                        finalComment.append(w).append(" ");
                    }
                }
                
                // Cập nhật lại cột nội dung bằng chuỗi đã làm sạch
                columns[1] = finalComment.toString().trim();
                
                // Ghép lại thành dòng data chuẩn bằng dấu chấm phẩy
                String outputLine = String.join(";", columns);
                context.write(NullWritable.get(), new Text(outputLine));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 1: Clean Data");
        job.setJarByClass(CleanReviewApp.class);
        job.setMapperClass(CleanMapper.class);
        job.setNumReduceTasks(0); // Chỉ cần Map để làm sạch data
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[0]).toUri()); // stopwords.txt
        FileInputFormat.addInputPath(job, new Path(args[1])); // hotel-review.csv
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // output_cleaned

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
