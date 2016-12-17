package Service;

import DAO.DbHelper;
import DAO.DbHelperImpl;
import com.mongodb.client.AggregateIterable;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Set;

/**
 * Created by chongyizhou on 2016-11-16.
 */
public class calcSumByType implements calcSumBuilder {
    String[] regions = {"universityOfWaterloo", "benchwood", "upperbeechwood", "beechwoodwest", "clairhills", "vistahills", "columbiaforest", "westvale", "marplehills", "westmount", "uptownwaterloo", "cityofwaterloo", "laurelwood", "conservationmeadows", "northlakeshore", "lakeshore", "colonialacres", "lincolnvillage", "lincolnheights", "eastbridge", "lexington", "universitydowns", "kiwanispark"};
    private DbHelper db = new DbHelperImpl();
    String[] names = {"2011", "2012", "2013", "2014"};


    @Override
    // the return type is <adminRegion, <crimeType, amount>>
    public HashMap<String, HashMap<String, Integer>> getResult(String region) {
  

        HashMap<String, HashMap<String, Integer>> resultMap = new HashMap<String, HashMap<String, Integer>>();
        for (String name : names) {
            String collectionName = "crime_collection" + name;
            Iterable<Document> aggregateIterable = db.matchAndGroupByField(collectionName, "finalCallTypeDescription", new Document("adminRegion", region));
            for (Document document : aggregateIterable) {
                //System.out.println(document);
                String finalType = "";
                String crimeType = (String)document.get("_id");
                if (crimeType != "NULL" && crimeType.split("[\\W]").length >= 2) {
                    String[] typeList = crimeType.split("[\\W]");
                    for (int i = 1; i < typeList.length; i++) {
                        if (i == 1) {
                            finalType = typeList[i];
                        } else {
                            finalType = finalType + "_" + typeList[i];
                        }
                    }
                } else {
                    finalType = "NULL";
                }
                int amount = (int)document.get("count");
                //System.out.println(amount);
                if (resultMap.keySet().contains(finalType)) {
                    resultMap.get(finalType).put(name, amount);
                } else {
                    HashMap<String, Integer> recordValue = new HashMap<String, Integer>();
                    recordValue.put(name, amount);
                    resultMap.put(finalType, recordValue);
                }
            }
        }
        return resultMap;
    }

    @Override
    public void updateInDb() {
        List<Document> recordList = new ArrayList<Document>();
        for (String region : regions) {
            HashMap<String, HashMap<String, Integer>> resultMap = getResult(region);
            System.out.println(resultMap);
            Document recordValue = new Document();
            if (resultMap != null) {
                recordValue.putAll(resultMap);
            }
            Document record = new Document(region, recordValue);
            System.out.println(record);
            recordList.add(record);
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        db.insertRecord("crime_by_type", recordList);
        return;
    }

    public static void main(String[] args) {
        calcSumByType calculator = new calcSumByType();
        //System.out.println(calculator.getResult("universityOfWaterloo"));
        calculator.updateInDb();
    }
}
