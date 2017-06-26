import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PreProcess {

    public static void main(String[] args) {
        String fileName = args[0];
        if (fileName == null) {
            fileName = "jester_ratings.dat";
        }
        String filePath = "/home/vidura/Projects/PreProcessData/src/" + fileName;
        String formattedFilePath = "/home/vidura/Projects/PreProcessData/src/formattedTrain2.dat";
        double threshold = 5.0;
        double userLimit = 40000;
        List<String> filteredList = new ArrayList<>();
        Map<Integer, StringBuilder> patternMap = new HashMap();

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            filteredList = stream
                    .filter(line -> Double.parseDouble(line.split("\\s+")[2]) > threshold)
                    .filter(line -> Double.parseDouble(line.split("\\s+")[0]) < userLimit)
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        filteredList.forEach(s -> {
            String[] tokens = s.split("\\s+");
            int userId = Integer.parseInt(tokens[0]);
            if (patternMap.get(userId) == null) {
                patternMap.put(userId, new StringBuilder());
            }
            patternMap.put(userId, patternMap.get(userId).append(tokens[1] + " "));
        });

        patternMap.forEach((k, v) -> System.out.println(k + " " + v));

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(formattedFilePath))) {
            patternMap.forEach((k, v) -> {
                try {
                    writer.write(v.toString());
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
