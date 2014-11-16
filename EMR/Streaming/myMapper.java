/*
 *  15-619 Cloud Computing
 *  Project 1.2 - Elastic MapReduce
 *
 *  Name: Jiachen Li
 *  AndrewID: jiachenl
 *
 */

import java.io.*;

public class myMapper {
    
    static String[] special_pages = {
        "Media", "Special", "Talk", "User", "User_talk", 
        "Project", "Project_talk", "File", "File_talk", 
        "MediaWiki", "MediaWiki_talk", "Template", 
        "Template_talk", "Help", "Help_talk", "Category",
        "Category_talk", "Portal", "Wikipedia", "Wikipedia_talk"
    };
    
    static String[] extensions = {
        ".jpg", ".gif", ".png", ".JPG", ".GIF", ".PNG", ".txt", ".ico"
    };
    
    static String[] boilerplate_articles = {
        "404_error/", "Main_Page", "Hypertext_Transfer_Protocol", 
        "Favicon.ico", "Search"
    };

    public static void main(String[] args) throws IOException {        
        try {
            BufferedReader br = 
                         new BufferedReader(new InputStreamReader(System.in));
            
            // get the input file name to decode the info needed
            String filename = System.getenv("map_input_file");
            //String filename = input.substring(0, input.lastIndexOf('.'));
            String[] time = filename.split("-");
            String day = time[2];  // just save the info of day
            
            // read in the log by line
            String line = null;
            while ((line = br.readLine()) != null) {
                // line structure:
                // <name> <title> <num of accesses> <total data returned (bytes)>
                String[] parts = line.split(" ");
                
                // in case of empty line (just in case)
                if (parts.length != 4)
                    continue;
                
                // filter out all pages that are not english wikipedia
                if (!parts[0].equals("en"))
                    continue;
                
                // filter out all articles start with lowercase English characters
                if (Character.isLowerCase(parts[1].charAt(0)))
                    continue;
                
                // exclude any pages whose title starts with specific strings
                boolean toNext = false;
                for (String str : special_pages) {
                    if (parts[1].startsWith(str + ":")) {
                        toNext = true;
                        break;
                    }
                }
                if (toNext)
                    continue;
                
                // filter out image files
                toNext = false;
                for (String str : extensions) {
                    if (parts[1].endsWith(str)) {
                        toNext = true;
                        break;
                    }
                }
                if (toNext)
                    continue;
                
                // filter out boilerplate articles
                toNext = false;
                for (String str : boilerplate_articles) {
                    if (parts[1].equals(str)) {
                        toNext = true;
                        break;
                    }
                }
                if (toNext)
                    continue;
                
                // after filtering, print it out
                System.out.println(parts[1] + "\t" + day + parts[2]);
            }
            
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
