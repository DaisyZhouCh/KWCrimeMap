package DAO;

import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.Document;
import static java.util.Arrays.asList;
import com.mongodb.client.model.Filters;
import com.google.common.collect.*;
import java.util.List;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * Created by chongyizhou on 2016-11-15.
 */

public class DbHelperImpl implements DbHelper {

    private final String HOST = "localhost";
    private final int PORT = 27017;
    private final String db_name = "crime_database";
    private MongoClient mongo = null;
    private MongoCollection mongoCollection = null;

    public void closeMongo(){
        this.mongo.close();
    }

    public MongoCollection getMongoCollection () {
        return this.mongoCollection;
    }


    public MongoClient getConnection(){
        try{
            this.mongo = new MongoClient(HOST, PORT);
            return this.mongo;
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public MongoCollection getCollection(String collectionName){
        try{
            this.mongo = new MongoClient(HOST, PORT);
            this.mongoCollection = mongo.getDatabase(db_name).getCollection(collectionName);
            return this.mongoCollection;
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean updateNewField(MongoCollection mongoCollection, Document document, String newField, String newValue) {
        try{
            mongoCollection.updateOne(new Document(document), new Document("$set", new Document(newField, newValue)));
            return true;
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    // with different parameters
    public boolean updateNewField(MongoCollection mongoCollection, Document document, String newField, int newValue) {
        try{
            mongoCollection.updateOne(new Document(document), new Document("$set", new Document(newField, newValue)));
            return true;
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }



    @Override
    // if there exits a certain field, then choose it
    public FindIterable<Document> selectByField(String collection, String field) {
        //MongoClient mongo = null;
        FindIterable<Document> iterable = null;
        MongoDatabase database;
        try{
            if (mongo == null){
                mongo = getConnection();
            }
            database = this.mongo.getDatabase(this.db_name);
            this.mongoCollection = database.getCollection(collection);
            iterable = this.mongoCollection.find(Filters.exists(field));
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return iterable;
    }

    public static void main(String[] args)throws Exception {

        final String HOST = "localhost";
        final int PORT = 27017;
        String db_name = "crime_database";
        MongoClient mongo = null;
        FindIterable<Document> iterable = null;
        //mongo = new MongoClient(HOST, PORT);
        //MongoDatabase db = mongo.getDatabase(db_name);
        //db.getCollection("crime_collection2011").createIndex(new Document("reportedDateAndTime", "text"));
        //iterable = db.getCollection("crime_collection2011").find(new Document("patrolDivision", "WS"));
        //iterable = db.getCollection("crime_collection2011").find(new Document("$text",new Document("$search","Jan")));
        DbHelper db = new DbHelperImpl();
        //iterable = db.selectEqual("crime_collection2011", new Document("patrolDivision", "WS"));
        //DbHelper db = new DbHelperImpl();
        iterable = db.selectSearch("crime_collection2011", "reportedDateAndTime", "Jan");
        //FindIterable<Document> iterable = db.selectEqual("crime_collection2011", new Document("patrolDivision", "WS"));
        System.out.println(Iterables.size(iterable));
        //mongo.close();
    }



    @Override
    // specify equality conditions
    // the form for params should be a Document with <field, "the value that field equals to">
    // or maybe just a field
    public FindIterable<Document> selectEqual(String collection, Document params) {
        MongoClient mongo = null;
        FindIterable<Document> iterable = null;
        MongoDatabase database;
        try{
            mongo = getConnection();
            database = mongo.getDatabase(this.db_name);
            iterable = database.getCollection(collection).find(new Document(params));
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return iterable;
    }

    public int getFindIterableSize (FindIterable<Document> findIterable) {
        if (findIterable == null)
            return 0;
        return (int)Iterables.size(findIterable);
    }

    public  int getAggregateIterableSize (AggregateIterable<Document> aggregateIterable) {
        if (aggregateIterable == null)
            return 0;
        return (int)Iterables.size(aggregateIterable);
    }


    @Override
    // indexField is the field in collection that is to be set as index
    public FindIterable<Document> selectSearch(String collection, String indexField, String search) {
        FindIterable<Document> iterable = null;
        MongoDatabase database;
        try{
            mongo = getConnection();
            database = mongo.getDatabase(this.db_name);
            this.mongoCollection = database.getCollection(collection);
            this.mongoCollection.createIndex(new Document(indexField, "text"));
            iterable = this.mongoCollection.find(new Document("$text",new Document("$search",search)));
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return iterable;
    }

    // to search under another field's match
    public FindIterable<Document> selectSearchAndMatch(String collection, String indexField, String search, String matchField, String matchValue) {
        FindIterable<Document> iterable = null;
        MongoDatabase database;
        try{
            if (this.mongo == null) {
                this.mongo = getConnection();
            }
            database = mongo.getDatabase(this.db_name);
            this.mongoCollection = database.getCollection(collection);
            this.mongoCollection.createIndex(new Document(indexField, "text"));
            iterable = this.mongoCollection.find(new Document("$text",new Document("$search",search)).append(matchField, matchValue));
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return iterable;
    }

    @Override
    // return a list like { "field" : "Missing", "count" : 51 }
    public AggregateIterable<Document> groupByField(String collection, String field) {
        //MongoClient mongo = null;
        AggregateIterable<Document> iterable = null;
        MongoDatabase database;
        try{
            if (mongo == null){
                mongo = getConnection();
            }
            database = mongo.getDatabase(this.db_name);
            this.mongoCollection = database.getCollection(collection);
            iterable = this.mongoCollection.aggregate(asList(
                    new Document("$group", new Document("_id", "$" + field).append("count", new Document("$sum", "$forSum")))));
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return iterable;
    }



    @Override
    public AggregateIterable<Document> matchAndGroupByField(String collection, String field, Document params) {
        //MongoClient mongo = null;
        AggregateIterable<Document> iterable = null;
        MongoDatabase database;
        try{
            if (mongo == null){
                mongo = getConnection();
            }
            database = mongo.getDatabase(this.db_name);
            this.mongoCollection = database.getCollection(collection);
            iterable = this.mongoCollection.aggregate(asList(
                    new Document("$match", new Document(params)),
                    new Document("$group", new Document("_id", "$" + field).append("count", new Document("$sum", "$forSum")))));
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return iterable;
    }

    @Override
    public void insertRecord(String collection, List<Document> record) {
        MongoDatabase database;
        try{
            if (this.mongo == null) {
                MongoClientOptions options = MongoClientOptions.builder().connectTimeout(100000).socketTimeout(100000).build();
                this.mongo = new MongoClient("27017", options);
            }
            System.out.println(this.mongo.getMongoClientOptions());
            //this.mongo = new MongoClient();
            database = this.mongo.getDatabase(this.db_name);
            this.mongoCollection = database.getCollection(collection);
            this.mongoCollection.insertMany(record);
        }catch (Exception e) {
            e.printStackTrace();
            return;
        }finally{
            try{
                //mongo.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return;
    }
}
