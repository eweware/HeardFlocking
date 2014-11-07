package com.eweware.heardflocking.util;

import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.ServiceProperties;
import com.eweware.heardflocking.base.HeardDB;
import com.mongodb.*;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * Created by weihan on 10/24/14.
 */
public class TransferInfoData {
    public TransferInfoData(HeardDB db) {
        this.db = db;
        getGroups();
    }

    private HeardDB db;
    private HashMap<String, String> groupNames;

    private final boolean TEST_ONLY_TECH = ServiceProperties.TEST_ONLY_TECH;
    private final boolean USER_BLAH_INFO_TO_STATS = ServiceProperties.TransferInfoData.USER_BLAH_INFO_TO_STATS;

    private String servicePrefix = "[TransferInfoData] ";
    private String groupPrefix;

    public void execute() {
        try {
            System.out.println(servicePrefix + "start running");
            System.out.println();

            for (String groupId : groupNames.keySet()) {
                if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;
                groupPrefix = "[" + groupNames.get(groupId) + "] ";

                List<String> blahIdList = transferBlahInfo(groupId);
                transferUserGroupInfo(groupId);
                if (USER_BLAH_INFO_TO_STATS) transferUserBlahInfoToStats(blahIdList);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getGroups() {
        DBCursor cursor = db.getGroupsCol().find();
        groupNames = new HashMap<>();
        while (cursor.hasNext()) {
            BasicDBObject group = (BasicDBObject) cursor.next();
            groupNames.put(group.getObjectId(DBConst.Groups.ID).toString(), group.getString(DBConst.Groups.NAME));
        }
        cursor.close();
    }

    private List<String> transferBlahInfo(String groupId) {
        // read blah information from blahs
        List<String> blahIdList = new ArrayList<>();
        Cursor cursor = db.getBlahsCol().find(new BasicDBObject("G", groupId));
        int i = 1;
        while (cursor.hasNext()) {
            BasicDBObject blah = (BasicDBObject) cursor.next();
            BlahInfo blahInfo = new BlahInfo(blah);
            writeBlahInfo(blahInfo);
            blahIdList.add(blahInfo.blahId.toString());
            if (i % 100 == 0)
                System.out.printf("%s%s %-9d   blahdb.blahs -> infodb.blahInfo\n", servicePrefix, groupPrefix, i);
            i++;
        }
        cursor.close();
        System.out.printf("\n%s%s %-9d blahInfo transferred\n\n", servicePrefix, groupPrefix, i);

        return blahIdList;
    }

    private void transferUserGroupInfo(String groupId) {
        Cursor cursor = db.getUserGroupCol().find(new BasicDBObject("G", groupId));
        int i = 1;
        while (cursor.hasNext()) {
            BasicDBObject userGroup = (BasicDBObject) cursor.next();
            UserGroupInfo userGroupInfo = new UserGroupInfo(userGroup);
            writeUserGroupInfo(userGroupInfo);
            if (i % 100 == 0)
                System.out.printf("%s%s %-9d   userdb.usergroups -> infodb.userGroupInfo\n", servicePrefix, groupPrefix, i);
            i++;
        }
        cursor.close();
        System.out.printf("\n%s%s %-9d userGroupInfo transferred\n\n", servicePrefix, groupPrefix, i);
    }

    private void transferUserBlahInfoToStats(List<String> blahIdList) {
        int i = 1;
        for (String blahId : blahIdList) {
            // find all user activity for this blah
            Cursor cursor = db.getUserBlahInfoOldCol().find(new BasicDBObject("B", blahId));

            while (cursor.hasNext()) {
                BasicDBObject userBlahInfoOld = (BasicDBObject) cursor.next();
                UserBlahInfo userBlahInfo = new UserBlahInfo(userBlahInfoOld);
                // write into statsdb with fake date year=0 month=0 day=0
                writeUserBlahInfoToStats(userBlahInfo);
                if (i % 100 == 0)
                    System.out.printf("%s%s %-9d   userdb.userBlahInfo -> infodb.userBlahInfo\n", servicePrefix, groupPrefix, i);
                i++;
            }
            cursor.close();
        }
        System.out.printf("\n%s%s %-9d userblahstats transferred\n\n", servicePrefix, groupPrefix, i);
    }

    private class BlahInfo {
        private BlahInfo(BasicDBObject blah) {
            blahId = blah.getObjectId("_id");
            createTime = blah.getDate("c");
            authorId = new ObjectId(blah.getString("A"));
            groupId = new ObjectId(blah.getString("G"));

            text = blah.getString("T");
            typeId = new ObjectId(blah.getString("Y"));
            imageIds = (ArrayList<String>) blah.get("M");
            badgeIds = (ArrayList<ObjectId>) blah.get("B");
            matureFlag = blah.getBoolean("XXX");

        }
        private ObjectId blahId;
        private Date createTime;
        private ObjectId authorId;
        private ObjectId groupId;

        private String text;
        private ObjectId typeId;
        private ArrayList<String> imageIds;
        private ArrayList<ObjectId> badgeIds;
        private Boolean matureFlag;
    }

    private void writeBlahInfo(BlahInfo blahInfo) {
        BasicDBObject values = new BasicDBObject();
//        values.put(DBConst.BlahInfo.ID, blahInfo.blahId);
        values.put(DBConst.BlahInfo.AUTHOR_ID, blahInfo.authorId);
        values.put(DBConst.BlahInfo.GROUP_ID, blahInfo.groupId);
        values.put(DBConst.BlahInfo.CREATE_TIME, blahInfo.createTime);

        values.put(DBConst.BlahInfo.TEXT, blahInfo.text);
        values.put(DBConst.BlahInfo.TYPE_ID, blahInfo.typeId);
        if (blahInfo.imageIds != null) values.put(DBConst.BlahInfo.IMAGE_IDS, blahInfo.imageIds);
        if (blahInfo.badgeIds != null) values.put(DBConst.BlahInfo.BADGE_IDS, blahInfo.badgeIds);
        if (blahInfo.matureFlag != null) values.put(DBConst.BlahInfo.MATURE_FLAG, blahInfo.matureFlag);

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.ID, blahInfo.blahId);
        db.getBlahInfoCol().update(query, setter, true, false);
    }

    private class UserGroupInfo {
        private UserGroupInfo(BasicDBObject blah) {
            userId = new ObjectId(blah.getString("U"));
            groupId = new ObjectId(blah.getString("G"));
        }
        private ObjectId userId;
        private ObjectId groupId;
    }

    private void writeUserGroupInfo(UserGroupInfo userGroupInfo) {
        BasicDBObject values = new BasicDBObject();
        values.put(DBConst.UserGroupInfo.USER_ID, userGroupInfo.userId);
        values.put(DBConst.UserGroupInfo.GROUP_ID, userGroupInfo.groupId);

//        if (FAKE_NEW_ACTIVITY) {
//            Random rand = new Random();
//            int views = rand.nextInt(50);
//            int opens = views/2 > 0 ? rand.nextInt(views/2) : 0;
//            int comments = opens/2 > 0 ? rand.nextInt(opens/2) : 0;
//            int ups = opens/2 > 0 ? rand.nextInt(opens/2) : 0;
//            int downs = (opens/2 - ups) > 0 ? rand.nextInt(opens/2 - ups) : 0;
//            values.put(DBConstants.UserGroupInfo.NEW_VIEWS, views);
//            values.put(DBConstants.UserGroupInfo.NEW_OPENS, opens);
//            values.put(DBConstants.UserGroupInfo.NEW_COMMENTS, comments);
//            values.put(DBConstants.UserGroupInfo.NEW_UPVOTES, ups);
//            values.put(DBConstants.UserGroupInfo.NEW_DOWNVOTES, downs);
//        }

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.USER_ID, userGroupInfo.userId);
        query.append(DBConst.UserGroupInfo.GROUP_ID, userGroupInfo.groupId);
        db.getUserGroupInfoCol().update(query, setter, true, false);
    }

    private class UserBlahInfo {
        private UserBlahInfo(BasicDBObject userBlah) {
            blahId = new ObjectId(userBlah.getString("B"));
            userId = new ObjectId(userBlah.getString("U"));
            views = userBlah.getInt("V", 0);
            opens = userBlah.getInt("O", 0);
            comments = userBlah.getInt("C", 0);
            if (userBlah.getInt("P", 0) == 1) {
                upvotes = 1;
            }
            else if (userBlah.getInt("P", 0) == -1) {
                downvotes = 1;
            }
        }
        private ObjectId blahId;
        private ObjectId userId;
        private long views;
        private long opens;
        private long comments;
        private long upvotes;
        private long downvotes;
//        private long comment_upvotes;
//        private long comment_downvotes;
    }

    private void writeUserBlahInfoToStats(UserBlahInfo userBlahInfo) {
        BasicDBObject values = new BasicDBObject();
        values.put(DBConst.UserBlahStats.BLAH_ID, userBlahInfo.blahId);
        values.put(DBConst.UserBlahStats.USER_ID, userBlahInfo.userId);
        if (userBlahInfo.views > 0) values.put(DBConst.UserBlahStats.VIEWS, userBlahInfo.views);
        if (userBlahInfo.opens > 0) values.put(DBConst.UserBlahStats.OPENS, userBlahInfo.opens);
        if (userBlahInfo.comments > 0) values.put(DBConst.UserBlahStats.COMMENTS, userBlahInfo.comments);
        if (userBlahInfo.upvotes > 0) values.put(DBConst.UserBlahStats.UPVOTES, userBlahInfo.upvotes);
        if (userBlahInfo.downvotes > 0) values.put(DBConst.UserBlahStats.DOWNVOTES, userBlahInfo.downvotes);
//        if (userBlahInfo.comment_upvotes > 0) values.put(DBConst.UserBlahStats.COMMENT_UPVOTES, userBlahInfo.comment_upvotes);
//        if (userBlahInfo.comment_downvotes > 0) values.put(DBConst.UserBlahStats.COMMENT_DOWNVOTES, userBlahInfo.comment_downvotes);

        // a fake time point for old data
        values.put(DBConst.UserBlahStats.YEAR, 0);
        values.put(DBConst.UserBlahStats.MONTH, 0);
        values.put(DBConst.UserBlahStats.DAY, 0);

        BasicDBObject setter = new BasicDBObject("$set", values);
        BasicDBObject query = new BasicDBObject(DBConst.UserBlahStats.BLAH_ID, userBlahInfo.blahId);
        query.append(DBConst.UserBlahStats.USER_ID, userBlahInfo.userId);
        db.getUserBlahStatsCol().update(query, setter, true, false);
    }
}
