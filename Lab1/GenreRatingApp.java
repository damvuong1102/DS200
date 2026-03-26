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

public class GenreRatingApp {

    public static class GenreMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private HashMap<String, String[]> movieGenres = new HashMap<>();
        private Text genreKey = new Text();
        private DoubleWritable ratingVal = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0]))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 3) {
                        // Lấy ID là phần tử đầu tiên
                        String id = parts[0].trim();
                        // Lấy Thể loại là phần tử CUỐI CÙNG (tránh lỗi nếu Tên phim bị chứa dấu phẩy)
                        String genreStr = parts[parts.length - 1].trim();
                        movieGenres.put(id, genreStr.split("\\|")); 
                    }
                }
                br.close();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String movieId = parts[1].trim(); // ID phim nằm ở cột 1 của file ratings
                try {
                    double rating = Double.parseDouble(parts[2].trim());
                    String[] genres = movieGenres.get(movieId);
                    
                    if (genres != null) {
                        for (String g : genres) {
                            genreKey.set(g.trim());
                            ratingVal.set(rating);
                            context.write(genreKey, ratingVal);
                        }
                    }
                } catch (NumberFormatException e) { }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            String result = String.format("AverageRating: %.2f (TotalRatings: %d)", sum / count, count);
            context.write(key, new Text(result)); 
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 2: Genre Rating");
        job.setJarByClass(GenreRatingApp.class);
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
