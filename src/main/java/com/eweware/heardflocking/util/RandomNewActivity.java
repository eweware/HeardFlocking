package com.eweware.heardflocking.util;

import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.ServiceProperties;
import com.eweware.heardflocking.base.HeardDB;
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
    public RandomNewActivity(HeardDB db) {
        this.db = db;
        getGroups();
    }

    private HeardDB db;

    private HashMap<String, String> groupNames;

    private final boolean BLAH_NEW_ACTIVITY = ServiceProperties.RandomNewActivity.BLAH_NEW_ACTIVITY;
    private final boolean USER_NEW_ACTIVITY = ServiceProperties.RandomNewActivity.USER_NEW_ACTIVITY;

    private final long BLAH_WAIT_MILLIS = ServiceProperties.RandomNewActivity.BLAH_WAIT_MILLIS;
    private final long USER_WAIT_MILLIS = ServiceProperties.RandomNewActivity.USER_WAIT_MILLIS;
    private final double BLAH_NEW_ACT_PROBABILITY = ServiceProperties.RandomNewActivity.BLAH_NEW_ACT_PROBABILITY;
    private final double USER_NEW_ACT_PROBABILITY = ServiceProperties.RandomNewActivity.User_NEW_ACT_PROBABILITY;

    private final boolean TEST_ONLY_TECH = ServiceProperties.TEST_ONLY_TECH;

    private String servicePrefix = "[RandomNewActivity] ";

    public void execute() {
        try {
            // two threads for every group, one for blahInfo, one for userGroupInfo
            for (String groupId : groupNames.keySet()) {
                if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;
                String groupPrefix = "[" + groupNames.get(groupId) + "] ";
                // blahInfo
                Thread randomBlahInfo = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        if (BLAH_NEW_ACTIVITY) {
                            long threadId = Thread.currentThread().getId();
                            System.out.println(servicePrefix + groupPrefix + "Thread " + threadId + " start : blahInfo random new activity");
                            try {
                                insertRandomNewActivityBlahInfo(groupId);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
                randomBlahInfo.start();

                // userGroupInfo
                Thread randomUserGroupInfo = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        if (USER_NEW_ACTIVITY) {
                            long threadId = Thread.currentThread().getId();
                            System.out.println(servicePrefix + groupPrefix + "Thread " + threadId + " start : userGroupInfo random new activity");
                            try {
                                insertRandomNewActivityUserGroupInfo(groupId);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
                randomUserGroupInfo.start();

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

    private void insertRandomNewActivityBlahInfo(String groupId) throws InterruptedException {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        while (true) {
            BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.GROUP_ID, new ObjectId(groupId));
            DBCursor cursor = db.getBlahInfoCol().find(query);
            Random rand = new Random(new Date().getTime());
            while (cursor.hasNext()) {
                // add random new activity if above threshold
                if (rand.nextDouble() < BLAH_NEW_ACT_PROBABILITY) {
                    BasicDBObject blahInfo = (BasicDBObject) cursor.next();
                    ObjectId blahIdObj = blahInfo.getObjectId(DBConst.BlahInfo.ID);

                    BasicDBObject values = new BasicDBObject();

                    int views = rand.nextInt(50);
                    int opens = views / 2 > 0 ? rand.nextInt(views / 2) : 0;
                    int comments = opens > 0 ? rand.nextInt(opens) : 0;
                    int ups = opens > 0 ? rand.nextInt(opens) : 0;
                    int downs = (opens - ups) > 0 ? rand.nextInt(opens - ups) : 0;

                    String newActPrefix = DBConst.BlahInfo.NEW_ACTIVITY+".";
                    values.put(newActPrefix+ DBConst.BlahInfo.NEW_VIEWS, views);
                    values.put(newActPrefix+ DBConst.BlahInfo.NEW_OPENS, opens);
                    values.put(newActPrefix+ DBConst.BlahInfo.NEW_COMMENTS, comments);
                    values.put(newActPrefix+ DBConst.BlahInfo.NEW_UPVOTES, ups);
                    values.put(newActPrefix+ DBConst.BlahInfo.NEW_DOWNVOTES, downs);

                    BasicDBObject inc = new BasicDBObject("$inc", values);
                    BasicDBObject queryBlah = new BasicDBObject(DBConst.BlahInfo.ID, blahIdObj);
//                    blahInfoTestCol.update(queryBlah, inc, true, false);
                    db.getBlahInfoCol().update(queryBlah, inc);

                    //long threadId = Thread.currentThread().getId();
                    System.out.println(servicePrefix + groupPrefix + "[blah] " + blahIdObj.toString() + " add new activity");

                    // wait for a bit
                    Thread.sleep(BLAH_WAIT_MILLIS);
                }
            }
        }
    }

    private void insertRandomNewActivityUserGroupInfo(String groupId) throws InterruptedException {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        while (true) {
            BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.GROUP_ID, new ObjectId(groupId));
            DBCursor cursor = db.getUserGroupInfoCol().find(query);
            Random rand = new Random(new Date().getTime());
            while (cursor.hasNext()) {
                // add random new activity if above threshold
                if (rand.nextDouble() < USER_NEW_ACT_PROBABILITY) {
                    BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
                    ObjectId userIdObj = userGroupInfo.getObjectId(DBConst.UserGroupInfo.USER_ID);
                    ObjectId groupIdObj = userGroupInfo.getObjectId(DBConst.UserGroupInfo.GROUP_ID);

                    BasicDBObject values = new BasicDBObject();

                    int views = rand.nextInt(50);
                    int opens = views / 2 > 0 ? rand.nextInt(views / 2) : 0;
                    int comments = opens > 0 ? rand.nextInt(opens) : 0;
                    int ups = opens > 0 ? rand.nextInt(opens) : 0;
                    int downs = (opens - ups) > 0 ? rand.nextInt(opens - ups) : 0;

                    String newActPrefix = DBConst.UserGroupInfo.NEW_ACTIVITY+".";
                    values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_VIEWS, views);
                    values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_OPENS, opens);
                    values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_COMMENTS, comments);
                    values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_UPVOTES, ups);
                    values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_DOWNVOTES, downs);

                    BasicDBObject inc = new BasicDBObject("$inc", values);
                    BasicDBObject queryUserGroup = new BasicDBObject(DBConst.UserGroupInfo.USER_ID, userIdObj);
                    queryUserGroup.append(DBConst.UserGroupInfo.GROUP_ID, groupIdObj);
//                    userGroupInfoTestCol.update(queryUserGroup, inc, true, false);
                    db.getUserGroupInfoCol().update(queryUserGroup, inc);

                    //long threadId = Thread.currentThread().getId();
                    System.out.println(servicePrefix + groupPrefix + " [user] " + userIdObj.toString() + " add new activity");

                    // wait for a bit
                    Thread.sleep(USER_WAIT_MILLIS);
                }
            }
        }
    }
}
