import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class MovieRatingApp {

    public static class RatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text movieId = new Text();
        private DoubleWritable rating = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Giả định dữ liệu phân cách bằng dấu phẩy
            String[] parts = value.toString().split(","); 
            if (parts.length >= 3) {
                movieId.set(parts[1].trim());
                try {
                    rating.set(Double.parseDouble(parts[2].trim()));
                    context.write(movieId, rating);
                } catch (NumberFormatException e) { }
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private HashMap<String, String> movieMap = new HashMap<>();
        private String maxMovie = "";
        private double maxRating = 0.0;

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0]))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        movieMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
                br.close();
            }
        }

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = sum / count;
            String title = movieMap.getOrDefault(key.toString(), "Unknown Movie (" + key.toString() + ")");

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = title;
            }

            String result = String.format("AverageRating: %.2f (TotalRatings: %d)", avg, count);
            context.write(new Text(title), new Text(result));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String finalResult = maxMovie + " is the highest rated movie with an average rating of " 
                        + String.format("%.2f", maxRating) + " among movies with at least 5 ratings.";
                context.write(new Text("--- KẾT QUẢ ĐẶC BIỆT ---"), new Text(finalResult));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 1: Movie Rating");
        job.setJarByClass(MovieRatingApp.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // args[0]: movies.txt, args[1]: ratings_1.txt, args[2]: ratings_2.txt, args[3]: output
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
