import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class TopAspectApp {

    public static class AspectMapper extends Mapper<Object, Text, Text, Text> {
        private Text aspectKey = new Text();
        private Text sentimentVal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Tách cột bằng dấu chấm phẩy
            String[] parts = value.toString().split(";");
            if (parts.length >= 5) {
                String aspect = parts[3].trim();
                String sentiment = parts[4].trim().toLowerCase(); // Lấy positive / negative
                
                // Bỏ qua các dòng bị thiếu dữ liệu Aspect hoặc Sentiment
                if (!aspect.isEmpty() && (sentiment.equals("positive") || sentiment.equals("negative"))) {
                    aspectKey.set(aspect);
                    sentimentVal.set(sentiment);
                    context.write(aspectKey, sentimentVal);
                }
            }
        }
    }

    public static class AspectReducer extends Reducer<Text, Text, Text, Text> {
        // Khai báo các biến để lưu kỷ lục
        private String maxPosAspect = "";
        private int maxPosCount = 0;
        
        private String maxNegAspect = "";
        private int maxNegCount = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int posCount = 0;
            int negCount = 0;

            // Đếm số lượng positive và negative của khía cạnh hiện tại
            for (Text val : values) {
                if (val.toString().equals("positive")) {
                    posCount++;
                } else if (val.toString().equals("negative")) {
                    negCount++;
                }
            }

            // Ghi nhận chi tiết từng khía cạnh (Có thể ẩn đi nếu chỉ muốn xem Top 1)
            String detail = String.format("-> Positive: %d, Negative: %d", posCount, negCount);
            context.write(key, new Text(detail));

            // So sánh để tìm Quán quân Tích cực
            if (posCount > maxPosCount) {
                maxPosCount = posCount;
                maxPosAspect = key.toString();
            }
            
            // So sánh để tìm Quán quân Tiêu cực
            if (negCount > maxNegCount) {
                maxNegCount = negCount;
                maxNegAspect = key.toString();
            }
        }

        // Cleanup chạy 1 lần duy nhất vào cuối cùng để in ra kết quả chốt sổ
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("-----------------------------------------"), new Text(""));
            context.write(new Text("[KẾT QUẢ] Khía cạnh TÍCH CỰC nhất:"), new Text(maxPosAspect + " (" + maxPosCount + " lượt)"));
            context.write(new Text("[KẾT QUẢ] Khía cạnh TIÊU CỰC nhất:"), new Text(maxNegAspect + " (" + maxNegCount + " lượt)"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 3: Top Aspects by Sentiment");
        job.setJarByClass(TopAspectApp.class);
        job.setMapperClass(AspectMapper.class);
        job.setReducerClass(AspectReducer.class);
        
        // Bắt buộc set 1 Reducer để đảm bảo biến cục bộ lưu đúng giá trị Max lớn nhất toàn hệ thống
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Đầu vào có thể dùng file gốc hoặc file output_cleaned từ bài 1 đều được
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
