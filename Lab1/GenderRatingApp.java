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

public class GenderRatingApp {

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> userGender = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    if (uri.toString().contains("users.txt")) {
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] parts = line.split(",");
                            if (parts.length >= 2) userGender.put(parts[0].trim(), parts[1].trim());
                        }
                        br.close();
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();
                String gender = userGender.getOrDefault(userId, "Unknown");
                
                context.write(new Text(movieId), new Text(gender + "_" + rating));
            }
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    if (uri.toString().contains("movies.txt")) {
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] parts = line.split(",");
                            if (parts.length >= 3) {
                                String id = parts[0].trim();
                                StringBuilder title = new StringBuilder();
                                for(int i=1; i<parts.length-1; i++) {
                                    title.append(parts[i]).append(i == parts.length-2 ? "" : ",");
                                }
                                movieMap.put(id, title.toString().trim());
                            }
                        }
                        br.close();
                    }
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumM = 0, sumF = 0;
            int countM = 0, countF = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("_");
                if (parts.length == 2) {
                    try {
                        double r = Double.parseDouble(parts[1]);
                        if (parts[0].equalsIgnoreCase("M")) { sumM += r; countM++; }
                        else if (parts[0].equalsIgnoreCase("F")) { sumF += r; countF++; }
                    } catch (Exception e) {}
                }
            }

            double avgM = countM > 0 ? sumM / countM : 0;
            double avgF = countF > 0 ? sumF / countF : 0;
            String title = movieMap.getOrDefault(key.toString(), "Unknown Movie (" + key.toString() + ")");
            
            String result = String.format("Male_Avg: %.2f, Female_Avg: %.2f", avgM, avgF);
            context.write(new Text(title), new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 3: Gender Rating");
        job.setJarByClass(GenderRatingApp.class);
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Nạp cả 2 file phụ vào Cache
        job.addCacheFile(new Path(args[0]).toUri()); // movies.txt
        job.addCacheFile(new Path(args[1]).toUri()); // users.txt
        FileInputFormat.addInputPath(job, new Path(args[2])); // ratings_1
        FileInputFormat.addInputPath(job, new Path(args[3])); // ratings_2
        FileOutputFormat.setOutputPath(job, new Path(args[4])); // output_task3

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
