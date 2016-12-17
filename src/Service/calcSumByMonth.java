package Service;

import DAO.DbHelper;
import DAO.DbHelperImpl;
import com.mongodb.client.FindIterable;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by chongyizhou on 2016-11-16.
 */
public class calcSumByMonth implements calcSumBuilder {
    // data is a HashMap to record the total of each year's crime records in each administrative region
    //example <"crime_collection2011": {"lexington": xxx, "laurelwood": xxx}>
    //outer layer represents name of collection, and inner layer represents records sum for each administrative region
    private HashMap<String, HashMap<String, Integer>> data = new HashMap<String, HashMap<String, Integer>>();
    private DbHelper db = new DbHelperImpl();
    String[] names = {"2011", "2012", "2013", "2014"};
    String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jn", "Jl", "Ag", "Sep", "Oct", "Nov", "Dec"};
    String[] regions = {"universityOfWaterloo", "benchwood", "upperbeechwood", "beechwoodwest", "clairhills", "vistahills", "columbiaforest", "westvale", "marplehills", "westmount", "uptownwaterloo", "cityofwaterloo", "laurelwood", "conservationmeadows", "northlakeshore", "lakeshore", "colonialacres", "lincolnvillage", "lincolnheights", "eastbridge", "lexington", "universitydowns", "kiwanispark"};

    @Override
    public HashMap<String, HashMap<String, Integer>> getResult(String region) {
        HashMap<String, HashMap<String, Integer>> result = new HashMap<String, HashMap<String, Integer>>();
        DbHelper db = this.db;
        for (String name : names) {
            String collectionName = "crime_collection" + name;
            System.out.println(collectionName);
            HashMap<String, Integer> resultYear = new HashMap<String, Integer>();
            for (String month : months) {
                FindIterable<Document> findIterable = db.selectSearchAndMatch(collectionName, "reportedDateAndTime", month, "adminRegion", region);
                int sum = db.getFindIterableSize(findIterable);
                resultYear.put(month, sum);
            }
            result.put(name, resultYear);
        }
        return result;
    }

    @Override
    // to put adminRegion in outer loop, and then insert each map into a new collection
    public void updateInDb() {
        DbHelper db = this.db;
        List<Document> recordList = new ArrayList<Document>();
        for (String region : regions) {
            HashMap<String, HashMap<String, Integer>> resultMap = getResult(region);
            Document reocordValue = new Document();
            reocordValue.putAll(resultMap);
            System.out.println(resultMap);
            Document record = new Document(region, reocordValue);
            System.out.println(record);
            recordList.add(record);
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //db.insertRecord("crime_by_year", record);
        }
        db.insertRecord("crime_by_year", recordList);
        System.out.println(recordList);
        return;
    }

    public static void main (String[] args) {
        calcSumByMonth calculator = new calcSumByMonth();
        calculator.updateInDb();

        //System.out.println(calculator.getResult("benchwood"));

    }
}
