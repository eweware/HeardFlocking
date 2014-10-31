package com.eweware.heardflocking;

import com.mongodb.*;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by weihan on 10/30/14.
 */
public class RandomNewActivity {
    public static void main(String[] args) {
        new RandomNewActivity().execute();
    }

    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;
    private DB blahDB;
    private DB statsDB;

    private DBCollection groupsCol;
    private DBCollection userBlahInfoOldCol;
    private DBCollection userGroupCol;

    private DBCollection blahInfoCol;
    private DBCollection cohortInfoCol;
    private DBCollection generationInfoCol;
    private DBCollection userGroupInfoCol;

    private DBCollection userBlahStatsCol;

    private DBCollection blahsCol;

    private DBCollection blahInfoTestCol;

    private HashMap<String, String> groupNames;

    private final long WAIT_BTW_BLAH_MILLIS = 1000;
    private final double BLAH_NEW_ACT_PROBABILITY = 0.01;

    private void execute() {
        try {
            initializeMongoDB();

            // two threads for every group, one for blahInfo, one for userGroupInfo
            for (String groupId : groupNames.keySet()) {
                if (!groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

                // blahInfo
                Thread randomBlahInfo = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        long threadId = Thread.currentThread().getId();
                        System.out.println("Thread " + threadId + " start : blahInfo random new activity");
                        try {
                            insertRandomNewActivityBlahInfo(groupId);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                randomBlahInfo.start();

                // userGroupInfo
                Thread randomUserGroupInfo = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        long threadId = Thread.currentThread().getId();
                        System.out.println("Thread " + threadId + " start : userGroupInfo random new activity");
                        insertRandomNewActivityUserGroupInfo(groupId);
                    }
                });
                randomUserGroupInfo.start();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeMongoDB() throws UnknownHostException {
        System.out.print("Initializing MongoDB connection...");

        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");
        blahDB = mongoClient.getDB("blahdb");
        statsDB = mongoClient.getDB("statsdb");

        groupsCol = userDB.getCollection("groups");
        userBlahInfoOldCol = userDB.getCollection("userBlahInfo");
        userGroupCol = userDB.getCollection("usergroups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");

        userBlahStatsCol = statsDB.getCollection("userblahstats");

        blahsCol = blahDB.getCollection("blahs");

        blahInfoTestCol = infoDB.getCollection("blahInfoTest");

        System.out.println("done");
        getGroups();
    }

    private void getGroups() {
        DBCursor cursor = groupsCol.find();
        groupNames = new HashMap<>();
        while (cursor.hasNext()) {
            BasicDBObject group = (BasicDBObject) cursor.next();
            groupNames.put(group.getObjectId(DBConstants.Groups.ID).toString(), group.getString(DBConstants.Groups.NAME));
        }
        cursor.close();
    }

    private void insertRandomNewActivityBlahInfo(String groupId) throws InterruptedException {
        while (true) {
            BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.GROUP_ID, new ObjectId(groupId));
            DBCursor cursor = blahInfoCol.find(query);
            Random rand = new Random(new Date().getTime());
            while (cursor.hasNext()) {
                // add random new activity if above threshold
                if (rand.nextDouble() < BLAH_NEW_ACT_PROBABILITY) {
                    BasicDBObject blahInfo = (BasicDBObject) cursor.next();
                    ObjectId blahIdObj = blahInfo.getObjectId(DBConstants.BlahInfo.ID);

                    BasicDBObject values = new BasicDBObject();

                    int views = rand.nextInt(50);
                    int opens = views / 2 > 0 ? rand.nextInt(views / 2) : 0;
                    int comments = opens / 2 > 0 ? rand.nextInt(opens / 2) : 0;
                    int ups = opens / 2 > 0 ? rand.nextInt(opens / 2) : 0;
                    int downs = (opens / 2 - ups) > 0 ? rand.nextInt(opens / 2 - ups) : 0;
                    values.put(DBConstants.BlahInfo.NEW_VIEWS, views);
                    values.put(DBConstants.BlahInfo.NEW_OPENS, opens);
                    values.put(DBConstants.BlahInfo.NEW_COMMENTS, comments);
                    values.put(DBConstants.BlahInfo.NEW_UPVOTES, ups);
                    values.put(DBConstants.BlahInfo.NEW_DOWNVOTES, downs);

                    BasicDBObject inc = new BasicDBObject("$inc", values);
                    BasicDBObject queryBlah = new BasicDBObject(DBConstants.BlahInfo.ID, blahIdObj);
                    blahInfoTestCol.update(queryBlah, inc, true, false);

                    long threadId = Thread.currentThread().getId();
                    System.out.println("Thread " + threadId + " add new activity into blah " + blahIdObj.toString());

                    // wait for a bit
                    Thread.sleep(WAIT_BTW_BLAH_MILLIS);
                }
            }
        }
    }

    private void insertRandomNewActivityUserGroupInfo(String groupId) {

    }
}
