package com.eweware.heardflocking;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.*;

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

    private HashMap<String, String> groupNames;

    private final boolean FAKE_NEW_ACTIVITY = true;

    private void execute() {
        try {
            initializeMongoDB();

//            transferUserBlahInfo();

            for (String groupId : groupNames.keySet()) {
                if (!groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

                transferBlahInfo(groupId);
//                transferUserGroupInfo(groupId);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeMongoDB() throws UnknownHostException {
        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DEV_DB_SERVER_PORT);
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

        Cursor cursor = groupsCol.find();
        groupNames = new HashMap<String, String>();
        while (cursor.hasNext()) {
            BasicDBObject group = (BasicDBObject) cursor.next();
            groupNames.put(group.getObjectId("_id").toString(), group.getString("N"));
        }
        cursor.close();
    }

    private void transferBlahInfo(String groupId) {
        // read blah information from blahs
        Cursor cursor = blahsCol.find(new BasicDBObject("G", groupId));
        int i = 1;
        while (cursor.hasNext()) {
            BasicDBObject blah = (BasicDBObject) cursor.next();
            BlahInfo blahInfo = new BlahInfo(blah);
            writeBlahInfo(blahInfo);
            if (i % 100 == 0)
                System.out.println(i + " documents transferred from blahdb.blahs to infodb.blahInfo");
            i++;
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
        System.out.println("done. " + i + " documents transferred.");
    }

    private void transferUserGroupInfo(String groupId) {
        Cursor cursor = userGroupCol.find(new BasicDBObject("G", groupId));
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
        System.out.println("done. " + i + " documents transferred.");
    }

    private class BlahInfo {
        private BlahInfo(BasicDBObject blah) {
            blahId = blah.getObjectId("_id").toString();
            createTime = blah.getDate("c");
            authorId = blah.getString("A");
            groupId = blah.getString("G");
            opens = blah.getInt("O", 0);

            text = blah.getString("T");
            typeId = blah.getString("Y");
            imageIds = (ArrayList<String>) blah.get("M");
            badgeIds = (ArrayList<String>) blah.get("B");
            matureFlag = blah.getBoolean("XXX");

        }
        private String blahId;
        private Date createTime;
        private String authorId;
        private String groupId;
        private int opens;

        private String text;
        private String typeId;
        private ArrayList<String> imageIds;
        private ArrayList<String> badgeIds;
        private Boolean matureFlag;
    }

    private void writeBlahInfo(BlahInfo blahInfo) {
        BasicDBObject values = new BasicDBObject();
        values.put(DBConstants.BlahInfo.ID, blahInfo.blahId);
        values.put(DBConstants.BlahInfo.AUTHOR_ID, blahInfo.authorId);
        values.put(DBConstants.BlahInfo.GROUP_ID, blahInfo.groupId);
        values.put(DBConstants.BlahInfo.CREATE_TIME, blahInfo.createTime);

        values.put(DBConstants.BlahInfo.TEXT, blahInfo.text);
        values.put(DBConstants.BlahInfo.TYPE_ID, blahInfo.typeId);
        if (blahInfo.imageIds != null) values.put(DBConstants.BlahInfo.IMAGE_IDS, blahInfo.imageIds);
        if (blahInfo.badgeIds != null) values.put(DBConstants.BlahInfo.BADGE_IDS, blahInfo.badgeIds);
        if (blahInfo.matureFlag != null) values.put(DBConstants.BlahInfo.MATURE_FLAG, blahInfo.matureFlag);

        if (FAKE_NEW_ACTIVITY && blahInfo.opens > 0) {
            Random rand = new Random();
            int views = rand.nextInt(50);
            int opens = views/2 > 0 ? rand.nextInt(views/2) : 0;
            int comments = opens/2 > 0 ? rand.nextInt(opens/2) : 0;
            int ups = opens/2 > 0 ? rand.nextInt(opens/2) : 0;
            int downs = (opens/2 - ups) > 0 ? rand.nextInt(opens/2 - ups) : 0;
            values.put(DBConstants.BlahInfo.NEW_VIEWS, views);
            values.put(DBConstants.BlahInfo.NEW_OPENS, opens);
            values.put(DBConstants.BlahInfo.NEW_COMMENTS, comments);
            values.put(DBConstants.BlahInfo.NEW_UPVOTES, ups);
            values.put(DBConstants.BlahInfo.NEW_DOWNVOTES, downs);
        }

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.ID, blahInfo.blahId);
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
        values.put(DBConstants.UserBlahStats.BLAH_ID, userBlahInfo.blahId);
        values.put(DBConstants.UserBlahStats.USER_ID, userBlahInfo.userId);
        if (userBlahInfo.views > 0) values.put(DBConstants.UserBlahStats.VIEWS, userBlahInfo.views);
        if (userBlahInfo.opens > 0) values.put(DBConstants.UserBlahStats.OPENS, userBlahInfo.opens);
        if (userBlahInfo.comments > 0) values.put(DBConstants.UserBlahStats.COMMENTS, userBlahInfo.comments);
        if (userBlahInfo.promotion > 0) values.put(DBConstants.UserBlahStats.PROMOTION, userBlahInfo.promotion);

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConstants.UserBlahStats.BLAH_ID, userBlahInfo.blahId);
        query.append(DBConstants.UserBlahStats.USER_ID, userBlahInfo.userId);
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
            int views = rand.nextInt(50);
            int opens = views/2 > 0 ? rand.nextInt(views/2) : 0;
            int comments = opens/2 > 0 ? rand.nextInt(opens/2) : 0;
            int ups = opens/2 > 0 ? rand.nextInt(opens/2) : 0;
            int downs = (opens/2 - ups) > 0 ? rand.nextInt(opens/2 - ups) : 0;
            values.put(DBConstants.UserGroupInfo.NEW_VIEWS, views);
            values.put(DBConstants.UserGroupInfo.NEW_OPENS, opens);
            values.put(DBConstants.UserGroupInfo.NEW_COMMENTS, comments);
            values.put(DBConstants.UserGroupInfo.NEW_UPVOTES, ups);
            values.put(DBConstants.UserGroupInfo.NEW_DOWNVOTES, downs);
        }

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, userGroupInfo.userId);
        query.append(DBConstants.UserGroupInfo.GROUP_ID, userGroupInfo.groupId);
        userGroupInfoCol.update(query, setter, true, false);
    }
}
