package com.eweware.heardflocking;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.util.HashMap;

/**
 * Created by weihan on 10/10/14.
 */
public class Main {
    public static void main(String[] args) throws UnknownHostException {
        String server = "localhost";
        int port = 21191;

        mongoClient = new MongoClient(server, port);

        getGroupNames();
        //String groupId = "522ccb78e4b0a35dadfcf73f";

        for (String groupId : groupNames.keySet()) {
            new CohortClustering(server, port, groupId).run();
        }

        //new CohortClusteringAllGroup().run();
    }
    private static MongoClient mongoClient;
    private static HashMap<String, String> groupNames = new HashMap<String, String>();
    private static void getGroupNames() {

        DBCollection groupsColl = mongoClient.getDB("userdb").getCollection("groups");
        DBCursor cursor = groupsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }
}
