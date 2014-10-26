package com.eweware.heardflocking;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Random;

/**
 * Created by weihan on 10/24/14.
 */
public class TransferInfoData {
    public static void main(String[] args) {
        new TransferInfoData().execute();
    }

    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;
    private DB blahDB;

    private DBCollection groupsCol;
    private DBCollection userBlahInfoOldCol;
    private DBCollection userGroupCol;

    private DBCollection blahInfoCol;
    private DBCollection cohortInfoCol;
    private DBCollection generationInfoCol;
    private DBCollection userGroupInfoCol;
    private DBCollection userBlahInfoCol;

    private DBCollection blahsCol;

    private final boolean FAKE_NEW_ACTIVITY = true;

    private void execute() {
        try {
            initializeMongoDB();
//            transferBlahInfo();
//            transferUserBlahInfo();
            transferUserGroupInfo();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeMongoDB() throws UnknownHostException {
        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");
        blahDB = mongoClient.getDB("blahdb");

        groupsCol = userDB.getCollection("groups");
        userBlahInfoOldCol = userDB.getCollection("userBlahInfo");
        userGroupCol = userDB.getCollection("usergroups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");
        userBlahInfoCol = infoDB.getCollection("userBlahInfo");

        blahsCol = blahDB.getCollection("blahs");

    }

    private void transferBlahInfo() {
        // read blah information from blahs
        Cursor cursor = blahsCol.find();
        int i = 1;
        while (cursor.hasNext()) {
            BasicDBObject blah = (BasicDBObject) cursor.next();
            BlahInfo blahInfo = new BlahInfo(blah);
            writeBlahInfo(blahInfo);
            System.out.println(i++ + " documents transferred from blahdb.blahs to infodb.blahInfo");
        }
        cursor.close();
        System.out.println("done. " + i + "documents transferred.");
    }

    private void transferUserBlahInfo() {
        Cursor cursor = userBlahInfoOldCol.find();
        int i = 1;
        while (cursor.hasNext()) {
            BasicDBObject userBlahInfoOld = (BasicDBObject) cursor.next();
            UserBlahInfo userBlahInfo = new UserBlahInfo(userBlahInfoOld);
            writeUserBlahInfo(userBlahInfo);
            if (i % 100 == 0)
                System.out.println(i + " documents transferred from userdb.userBlahInfo to infodb.userBlahInfo");
            i++;
        }
        cursor.close();
        System.out.println("done. " + i + "documents transferred.");
    }

    private void transferUserGroupInfo() {
        Cursor cursor = userGroupCol.find();
        int i = 1;
        while (cursor.hasNext()) {
            BasicDBObject userGroup = (BasicDBObject) cursor.next();
            UserGroupInfo userGroupInfo = new UserGroupInfo(userGroup);
            writeUserGroupInfo(userGroupInfo);
            if (i % 500 == 0)
                System.out.println(i + " documents transferred from userdb.usergroups to infodb.userGroupInfo");
            i++;
        }
        cursor.close();
        System.out.println("done. " + i + "documents transferred.");
    }

    private class BlahInfo {
        private BlahInfo(BasicDBObject blah) {
            blahId = blah.getObjectId("_id").toString();
            createTime = blah.getDate("c");
            authorId = blah.getString("A");
            groupId = blah.getString("G");
        }
        private String blahId;
        private Date createTime;
        private String authorId;
        private String groupId;
    }

    private void writeBlahInfo(BlahInfo blahInfo) {
        BasicDBObject values = new BasicDBObject();
        values.put(DBConstants.BlahInfo.BLAH_ID, blahInfo.blahId);
        values.put(DBConstants.BlahInfo.AUTHOR_ID, blahInfo.authorId);
        values.put(DBConstants.BlahInfo.GROUP_ID, blahInfo.groupId);
        values.put(DBConstants.BlahInfo.CREATE_TIME, blahInfo.createTime);

        if (FAKE_NEW_ACTIVITY) {
            Random rand = new Random();
            values.put(DBConstants.BlahInfo.NEW_VIEWS, rand.nextInt(30));
            values.put(DBConstants.BlahInfo.NEW_OPENS, rand.nextInt(10));
            values.put(DBConstants.BlahInfo.NEW_COMMENTS, rand.nextInt(3));
            values.put(DBConstants.BlahInfo.NEW_UPVOTES, rand.nextInt(2));
            values.put(DBConstants.BlahInfo.NEW_DOWNVOTES, rand.nextInt(2));
        }

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.BLAH_ID, blahInfo.blahId);
        blahInfoCol.update(query, setter, true, false);
    }

    private class UserBlahInfo {
        private UserBlahInfo(BasicDBObject userBlah) {
            blahId = userBlah.getString("B");
            userId = userBlah.getString("U");
//            authorId = userBlah.getString("A");
//            groupId = userBlah.getString("G");
            views = userBlah.getInt("V", 0);
            opens = userBlah.getInt("O", 0);
            comments = userBlah.getInt("C", 0);
            promotion = userBlah.getInt("P", 0);
        }
        private String blahId;
        private String userId;
//        private String authorId;
//        private String groupId;
        private int views;
        private int opens;
        private int comments;
        private int promotion;
    }

    private void writeUserBlahInfo(UserBlahInfo userBlahInfo) {
        BasicDBObject values = new BasicDBObject();
        values.put(DBConstants.UserBlahInfo.BLAH_ID, userBlahInfo.blahId);
        values.put(DBConstants.UserBlahInfo.USER_ID, userBlahInfo.userId);
        if (userBlahInfo.views > 0) values.put(DBConstants.UserBlahInfo.VIEWS, userBlahInfo.views);
        if (userBlahInfo.opens > 0) values.put(DBConstants.UserBlahInfo.OPENS, userBlahInfo.opens);
        if (userBlahInfo.comments > 0) values.put(DBConstants.UserBlahInfo.COMMENTS, userBlahInfo.comments);
        if (userBlahInfo.promotion > 0) values.put(DBConstants.UserBlahInfo.PROMOTION, userBlahInfo.promotion);

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConstants.UserBlahInfo.BLAH_ID, userBlahInfo.blahId);
        query.append(DBConstants.UserBlahInfo.USER_ID, userBlahInfo.userId);
        userBlahInfoCol.update(query, setter, true, false);
    }

    private class UserGroupInfo {
        private UserGroupInfo(BasicDBObject blah) {
            userId = blah.getString("U");
            groupId = blah.getString("G");
        }
        private String userId;
        private String groupId;
    }

    private void writeUserGroupInfo(UserGroupInfo userGroupInfo) {
        BasicDBObject values = new BasicDBObject();
        values.put(DBConstants.UserGroupInfo.USER_ID, userGroupInfo.userId);
        values.put(DBConstants.UserGroupInfo.GROUP_ID, userGroupInfo.groupId);

        if (FAKE_NEW_ACTIVITY) {
            Random rand = new Random();
            values.put(DBConstants.UserGroupInfo.NEW_VIEWS, rand.nextInt(30));
            values.put(DBConstants.UserGroupInfo.NEW_OPENS, rand.nextInt(10));
            values.put(DBConstants.UserGroupInfo.NEW_COMMENTS, rand.nextInt(3));
            values.put(DBConstants.UserGroupInfo.NEW_UPVOTES, rand.nextInt(2));
            values.put(DBConstants.UserGroupInfo.NEW_DOWNVOTES, rand.nextInt(2));
        }

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, userGroupInfo.userId);
        query.append(DBConstants.UserGroupInfo.GROUP_ID, userGroupInfo.groupId);
        userGroupInfoCol.update(query, setter, true, false);
    }
}
