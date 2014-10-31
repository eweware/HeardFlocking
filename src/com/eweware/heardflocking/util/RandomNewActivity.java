package com.eweware.heardflocking.util;

import com.eweware.heardflocking.DBConstants;
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
    public RandomNewActivity(String server) {
        DB_SERVER = server;
    }

    public static void main(String[] args) {
        // MongoDB server configuration
        String server = DBConstants.DEV_DB_SERVER;
        if (args.length > 0) {
            if (args[0].equals("dev"))
                server = DBConstants.DEV_DB_SERVER;
            else if (args[0].equals("qa"))
                server = DBConstants.QA_DB_SERVER;
            else if (args[0].equals("prod"))
                server = DBConstants.PROD_DB_SERVER;
            else
            {}
        }

        new RandomNewActivity(server).execute();
    }

    private String DB_SERVER;
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

    private DBCollection blahInfoTestCol;
    private DBCollection userGroupInfoTestCol;

    private DBCollection userBlahStatsCol;

    private DBCollection blahsCol;

    private HashMap<String, String> groupNames;

    private final long BLAH_WAIT_MILLIS = 100;
    private final double BLAH_NEW_ACT_PROBABILITY = 0.01;
    private final long USER_WAIT_MILLIS = 100;
    private final double USER_NEW_ACT_PROBABILITY = 0.01;

    private final boolean TEST_ONLY_TECH = false;

    private void execute() {
        try {
            initializeMongoDB();

            // two threads for every group, one for blahInfo, one for userGroupInfo
            for (String groupId : groupNames.keySet()) {
                if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

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
                        try {
                            insertRandomNewActivityUserGroupInfo(groupId);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
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

        mongoClient = new MongoClient(DB_SERVER, DBConstants.DB_SERVER_PORT);
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

        blahInfoTestCol = infoDB.getCollection("blahInfoTest");
        userGroupInfoTestCol = infoDB.getCollection("userGroupInfoTest");

        userBlahStatsCol = statsDB.getCollection("userblahstats");

        blahsCol = blahDB.getCollection("blahs");

        getGroups();

        System.out.println("done");
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
                    int comments = opens > 0 ? rand.nextInt(opens) : 0;
                    int ups = opens > 0 ? rand.nextInt(opens) : 0;
                    int downs = (opens - ups) > 0 ? rand.nextInt(opens - ups) : 0;

                    String newActPrefix = DBConstants.BlahInfo.NEW_ACTIVITY+".";
                    values.put(newActPrefix+DBConstants.BlahInfo.NEW_VIEWS, views);
                    values.put(newActPrefix+DBConstants.BlahInfo.NEW_OPENS, opens);
                    values.put(newActPrefix+DBConstants.BlahInfo.NEW_COMMENTS, comments);
                    values.put(newActPrefix+DBConstants.BlahInfo.NEW_UPVOTES, ups);
                    values.put(newActPrefix+DBConstants.BlahInfo.NEW_DOWNVOTES, downs);

                    BasicDBObject inc = new BasicDBObject("$inc", values);
                    BasicDBObject queryBlah = new BasicDBObject(DBConstants.BlahInfo.ID, blahIdObj);
//                    blahInfoTestCol.update(queryBlah, inc, true, false);
                    blahInfoCol.update(queryBlah, inc);

                    long threadId = Thread.currentThread().getId();
                    System.out.println("Thread " + threadId + " add new activity into blahInfo " + blahIdObj.toString());

                    // wait for a bit
                    Thread.sleep(BLAH_WAIT_MILLIS);
                }
            }
        }
    }

    private void insertRandomNewActivityUserGroupInfo(String groupId) throws InterruptedException {
        while (true) {
            BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.GROUP_ID, new ObjectId(groupId));
            DBCursor cursor = userGroupInfoCol.find(query);
            Random rand = new Random(new Date().getTime());
            while (cursor.hasNext()) {
                // add random new activity if above threshold
                if (rand.nextDouble() < USER_NEW_ACT_PROBABILITY) {
                    BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
                    ObjectId userIdObj = userGroupInfo.getObjectId(DBConstants.UserGroupInfo.USER_ID);
                    ObjectId groupIdObj = userGroupInfo.getObjectId(DBConstants.UserGroupInfo.GROUP_ID);

                    BasicDBObject values = new BasicDBObject();

                    int views = rand.nextInt(50);
                    int opens = views / 2 > 0 ? rand.nextInt(views / 2) : 0;
                    int comments = opens > 0 ? rand.nextInt(opens) : 0;
                    int ups = opens > 0 ? rand.nextInt(opens) : 0;
                    int downs = (opens - ups) > 0 ? rand.nextInt(opens - ups) : 0;

                    String newActPrefix = DBConstants.UserGroupInfo.NEW_ACTIVITY+".";
                    values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_VIEWS, views);
                    values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_OPENS, opens);
                    values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_COMMENTS, comments);
                    values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_UPVOTES, ups);
                    values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_DOWNVOTES, downs);

                    BasicDBObject inc = new BasicDBObject("$inc", values);
                    BasicDBObject queryUserGroup = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, userIdObj);
                    queryUserGroup.append(DBConstants.UserGroupInfo.GROUP_ID, groupIdObj);
//                    userGroupInfoTestCol.update(queryUserGroup, inc, true, false);
                    userGroupInfoCol.update(queryUserGroup, inc);

                    long threadId = Thread.currentThread().getId();
                    System.out.println("Thread " + threadId + " add new activity into userGroupInfo " + userIdObj.toString());

                    // wait for a bit
                    Thread.sleep(USER_WAIT_MILLIS);
                }
            }
        }
    }
}
