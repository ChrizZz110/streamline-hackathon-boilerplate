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

    public static ArrayList<String> extractTags(String input){
        String[] stringArray = input.split(" ");
        ArrayList<String> finalArrayList = new ArrayList<>();
        for(int i = 0; i<stringArray.length; i++){
            String tmp = stringArray[i];
            if(tmp.contains("#")){
                finalArrayList.add(stringArray[i]);
            }
        }
        return finalArrayList;
    }

    public static ArrayList<String> extractMentions(String input){
        String[] stringArray = input.split(" ");
        ArrayList<String> finalArrayList = new ArrayList<>();
        for(int i = 0; i<stringArray.length; i++){
            String tmp = stringArray[i];
            if(tmp.contains("@")){
                finalArrayList.add(stringArray[i]);
            }
        }
        return finalArrayList;
    }


}
