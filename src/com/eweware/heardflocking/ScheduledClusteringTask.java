package com.eweware.heardflocking;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.TimerTask;

/**
 * Created by weihan on 10/13/14.
 */
public class ScheduledClusteringTask extends TimerTask {
    public void run() {
        try {
            System.out.println();
            System.out.println("########## Start scheduled clustering task ##########  " + new Date());
            System.out.println();
            mongoClient = new MongoClient(DEV_DB_SERVER, DB_SERVER_PORT);

            getGroupNames();
            //groupNames.put("522ccb78e4b0a35dadfcf73f", "Tech Industry");

            for (String groupId : groupNames.keySet()) {
                System.out.print("Check need for clustering group '" + groupNames.get(groupId) + "' id : " + groupId + " ...");
                if (checkNeedClustering(groupId)) {
                    System.out.println("YES");
                    new CohortClustering(mongoClient, groupId).execute();
                }
                else {
                    System.out.println("NO");
                }
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println("error: can't connect to MongoDB server");
        }
    }

    private final String DEV_DB_SERVER = "localhost";
    private final String QA_DB_SERVER = "qa.db.blahgua.com";
    private final int DB_SERVER_PORT = 21191;

    private MongoClient mongoClient;
    private HashMap<String, String> groupNames = new HashMap<String, String>();

    private void getGroupNames() {
        DBCollection groupsColl = mongoClient.getDB("userdb").getCollection("groups");
        DBCursor cursor = groupsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private boolean checkNeedClustering(String groupId) {
        //TODO add condition to re-cluster cohorts for this group
        return true;
    }
}
