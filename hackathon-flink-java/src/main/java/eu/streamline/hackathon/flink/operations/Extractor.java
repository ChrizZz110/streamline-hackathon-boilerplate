package eu.streamline.hackathon.flink.operations;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Extractor {

    public static ArrayList<String> extract(String input){
        ArrayList<String> finalArrayList = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher(input);
        // check all occurance
        while (matcher.find()) {
            finalArrayList.add(matcher.group());
        }
        return finalArrayList;
    }
}
