package Service;

import java.util.HashMap;


/**
 * Created by chongyizhou on 2016-11-18.
 */
public interface calcSumBuilder {
    //eg. <year, <month, amount>>
    HashMap<String, HashMap<String, Integer>> getResult(String region);
    void updateInDb();

}
