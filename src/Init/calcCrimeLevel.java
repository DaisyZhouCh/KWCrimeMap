package Init;

import DAO.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.connection.Cluster;
import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import org.bson.Document;
import java.util.*;
import net.sf.javaml.clustering.mcl.*;
import net.sf.javaml.core.*;

/**
 * Created by chongyizhou on 2016-11-16.
 */
public class calcCrimeLevel {
    // data is a HashMap to record the total of each year's crime records in each administrative region
    //example <"crime_collection2011": {"lexington": xxx, "laurelwood": xxx}>
    //outer layer represents name of collection, and inner layer represents records sum for each administrative region
    private HashMap<String, HashMap<String, Integer>> data = new HashMap<String, HashMap<String, Integer>>();
    private DbHelper db = new DbHelperImpl();
    String[] collectionName = {"crime_collection2011", "crime_collection2012", "crime_collection2013", "crime_collection2014"};


    public HashMap<String, HashMap<String, Integer>> getData() {
        calcData();
        return this.data;
    }

    //return a HashMap to represent name of adminRegion and its color, eg. <"westmount", "yellow">
    public HashMap<Integer, Set<String>> getAdminMap () {
        basicKmeans kmeans = new basicKmeans();
        double[][] g = kmeans.getResult(getCrimeAmount());
        HashMap<Integer, Set<String>> result = new HashMap<Integer, Set<String>>();
        double[] storeMax = new double[g.length];
        HashMap<String, Integer> crimeAmount = getCrimeAmount();
        Set<String> regions = crimeAmount.keySet();

        for (int i = 0; i < g.length; i++) {
            for (int j = 0; j < (g[i].length-1); j++) {
                storeMax[i] = Math.max(g[i][j],g[i][j+1]);
            }
        }
        //this array will store each cluster's crime level
        int[] crimeLevel = new int[g.length];
        for (int i = 0; i < storeMax.length; i++) {
            crimeLevel[i] = 1;
            for (int j = 0; j < storeMax.length; j++) {
                if (j == i) {
                    continue;
                }
                if (storeMax[i] > storeMax[j]) {
                    crimeLevel[i] = crimeLevel[i] + 1;
                }
            }
        }

        for (int i = 0; i < g.length; i++) {
            HashSet<String> regionsInACluster = new HashSet<String>();
            for (int j = 0; j < g[i].length; j++) {
                for (String region : regions) {
                    if (crimeAmount.get(region) == g[i][j]) {
                        regionsInACluster.add(region);
                    }
                }
            }
            result.put(crimeLevel[i], regionsInACluster);
        }
        System.out.println(result);
        return result;
    }

    public void updateInDb() {
        List<Document> recordList = new ArrayList<Document>();
        HashMap<Integer, Set<String>> levelMap = getAdminMap();
        Set<Integer> levels = levelMap.keySet();
        for (Integer level : levels) {
            Set<String> regions = levelMap.get(level);
            for (String region : regions) {
                Document record = new Document(region, level);
                recordList.add(record);
            }
        }
        db.insertRecord("crime_level", recordList);
        return;
    }


    // use this function to return a crime amount HashMap, <adminRegion: crimeAmount>
    public HashMap<String, Integer> getCrimeAmount() {
        HashMap<String, HashMap<String, Integer>> data = getData();
        HashMap<String, Integer> crimeAmount = new HashMap<String, Integer>();
        for(String name : collectionName) {
            if (name == "crime_collection2011") {
                crimeAmount = data.get("crime_collection2011");
            }
            HashMap<String, Integer> regionsMap = data.get(name);
            Set<String> regions = regionsMap.keySet();
            for(String region : regions) {
                crimeAmount.put(region, crimeAmount.get(region) + regionsMap.get(region));
            }
        }
        return crimeAmount;
    }

    // with aggregation pipline in mongoDB
    public void calcData () {
        MongoCollection mongoCollection = null;
        for(String name : collectionName) {
            mongoCollection = db.getCollection(name);
            System.out.println(mongoCollection);
            HashMap<String, Integer> element = new HashMap<String, Integer>();
            // create pipeline operations, first with the $match
            Document match = new Document("$match", new Document("adminRegion", new Document("$ne", "NULL")));
            //the $group oepration
            Document groupFields = new Document("_id", "$adminRegion");
            groupFields.put("total", new Document("$sum", "$forSum"));
            Document group = new Document("$group", groupFields);
            //run aggregation
            Iterable<Document> output = mongoCollection.aggregate(Arrays.asList(match, group));
            //structure for a Document object is: Document{{_id=beechwoodwest, total=788}}
            for (Document document : output) {
                String regionName = (String)document.get("_id");
                int total = (int)document.get("total");
                try{
                    element.put(regionName, total);
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }
            }
            this.data.put(name, element);
        }
    }

    public static void main(String[] args) {
        calcCrimeLevel calc = new calcCrimeLevel();
        //calc.updateInDb();
    }


}
