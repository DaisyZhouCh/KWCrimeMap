package DAO;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Created by chongyizhou on 2016-11-15.
 */

public interface DbHelper {

    MongoCollection getCollection(String collectionName);
    MongoCollection getMongoCollection();
    MongoClient getConnection();
    void closeMongo();
    FindIterable<Document> selectByField(String collection, String field);
    int getFindIterableSize (FindIterable<Document> findIterable);
    FindIterable<Document> selectEqual(String collection, Document params);
    FindIterable<Document> selectSearch(String collection,String indexField, String search);
    FindIterable<Document> selectSearchAndMatch(String collection, String indexField, String search, String matchField, String matchValue);    AggregateIterable<Document> groupByField(String collection, String field);
    AggregateIterable<Document> matchAndGroupByField(String collection, String field, Document params);
    boolean updateNewField(MongoCollection mongoCollection, Document document, String newField, String newValue);
    boolean updateNewField(MongoCollection mongoCollection, Document document, String newField, int newValue);
    void insertRecord(String collection, List<Document> record) ;
}
