/*
 *  15-619 Cloud Computing
 *  Project 1.2 - Elastic MapReduce
 *
 *  Name: Jiachen Li
 *  AndrewID: jiachenl
 *
 */

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class myReducer {
    
    public static final int THRESHOLD = 100;
    
    public static void main(String[] args) throws IOException {        
        try {
            BufferedReader br = 
                         new BufferedReader(new InputStreamReader(System.in));
            
            String lastTitle = null;
            int ctViews = 0;
            Map<String, Integer> dayView = new HashMap<String, Integer>();
            
            // read-in key/value pair by line
            String line = null;
            while ((line = br.readLine()) != null) {
                // line structure: key/value pair
                // e.g. title \t 201407ddviews
                String[] parts = line.split("\t");
                //if (parts.length != 2)
                //    continue;
                String title = parts[0];
                String day = parts[1].substring(0, 8);
                int views = Integer.parseInt(parts[1].substring(8));
                
                if (title.equals(lastTitle)) {
                    // still the same article
                    ctViews += views;
                    if (dayView.containsKey(day))
                        dayView.put(day, dayView.get(day) + views);
                    else
                        dayView.put(day, views);
                }
                else {
                    if (ctViews > THRESHOLD) {
                        // if page view > 100000, print it out
                        System.out.print(ctViews + "\t" + lastTitle);
                        Iterator<String> iter = dayView.keySet().iterator();
                        while (iter.hasNext()) {
                            String key = iter.next();
                            System.out.print("\t" + key + ":" + dayView.get(key));
                        }
                        System.out.println();
                    }
                    lastTitle = title;  // re-set the lastTitle symbol
                    ctViews = views;
                    dayView.clear();
                    dayView.put(day, views);
                }
            }
            
            // output the last element
            if (!lastTitle.isEmpty() && ctViews > THRESHOLD) {
                // if page view > 100000, print it out
                System.out.print(ctViews + "\t" + lastTitle);
                Iterator<String> iter = dayView.keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    System.out.print("\t" + key + ":" + dayView.get(key));
                }
                System.out.println();
            }
            
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
