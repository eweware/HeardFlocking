package com.eweware.heardflocking.strength;

import com.eweware.heardflocking.*;
import com.eweware.heardflocking.ServiceProperties;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 10/22/14.
 */
public class StrengthMonitor extends TimerTask {

    public StrengthMonitor(String server, Date startTime, int periodHours) {
        DB_SERVER = server;
        this.startTime = startTime;
        this.periodHours = periodHours;
    }

    private CloudQueueClient queueClient;
    private CloudQueue strengthTaskQueue;

    private String DB_SERVER;
    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;

    private DBCollection groupsCol;
    private DBCollection blahInfoCol;
    private DBCollection userGroupInfoCol;

    private HashMap<String, String> groupNames;

    private final Date startTime;
    private int periodHours;

    private static final int PERIOD_HOURS = ServiceProperties.StrengthMonitor.PERIOD_HOURS;
    private final int RECENT_BLAH_MONTHS = ServiceProperties.StrengthMonitor.RECENT_BLAH_MONTHS;

    private final boolean TEST_ONLY_TECH = ServiceProperties.TEST_ONLY_TECH;

    private String servicePrefix = "[StrengthMonitor] ";

    public static void execute(String server) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set time to run
        cal.set(Calendar.HOUR_OF_DAY, ServiceProperties.StrengthMonitor.START_HOUR);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // set period
        System.out.println("[StrengthMonitor] start running, period=" + PERIOD_HOURS + " (hours), time : "  + new Date());

        timer.schedule(new StrengthMonitor(server, cal.getTime(), PERIOD_HOURS), cal.getTime(), TimeUnit.HOURS.toMillis(PERIOD_HOURS));
    }

    @Override
    public void run() {
        try {
            initializeMongoDB();
            initializeQueue();

            scanBlahs();
            scanUsers();

            Calendar nextTime = Calendar.getInstance();
            nextTime.setTime(startTime);
            nextTime.add(Calendar.HOUR, periodHours);
            System.out.println(servicePrefix + "next scan in less than " + periodHours + " hours at time : " + nextTime.getTime());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeQueue() throws Exception {
        System.out.print(servicePrefix + "initialize Azure Storage Queue service... ");

        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(AzureConstants.STORAGE_CONNECTION_STRING);

        // Create the queue client.
        queueClient = storageAccount.createCloudQueueClient();

        // Retrieve a reference to a queue.
        strengthTaskQueue = queueClient.getQueueReference(AzureConstants.STRENGTH_TASK_QUEUE);

        // Create the queue if it doesn't already exist.
        strengthTaskQueue.createIfNotExists();

        System.out.println("done");
    }

    private void initializeMongoDB() throws UnknownHostException {
        System.out.print(servicePrefix + "initialize MongoDB connection... ");

        mongoClient = new MongoClient(DB_SERVER, DBConstants.DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");

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

    private void scanBlahs() throws StorageException {
        System.out.println(servicePrefix + "start scanning blahs...");
        // only look at blahs created within certain number of months
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -RECENT_BLAH_MONTHS);
        Date earliestRelevantDate = cal.getTime();

        // scan by group
        for (String groupId : groupNames.keySet()) {
            if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

            String groupPrefix = "[" + groupNames.get(groupId) + "] ";

            // get all relevant blah info in this group
            BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.GROUP_ID, new ObjectId(groupId));
            query.put(DBConstants.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate));
            // "relevant" also means the "next check time" for the blah has passed
            List<BasicDBObject> orList = new ArrayList<>();
            orList.add(new BasicDBObject(DBConstants.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$lt", new Date())));
            orList.add(new BasicDBObject(DBConstants.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$exists", false)));
            query.put("$or", orList);
            Cursor cursor = blahInfoCol.find(query);

            while (cursor.hasNext()) {
                BasicDBObject blahInfo = (BasicDBObject) cursor.next();
                String blahId = blahInfo.getString(DBConstants.BlahInfo.ID);

                System.out.print(servicePrefix + groupPrefix + "check blah <" + blahId + "> in group '" + groupNames.get(groupId) + "'... ");

                if (blahIsActive(blahInfo)) {
                    System.out.print("active, producing task... ");

                    // produce re-compute strength task
                    BasicDBObject task = new BasicDBObject();
                    task.put(AzureConstants.StrengthTask.TYPE, AzureConstants.StrengthTask.COMPUTE_BLAH_STRENGTH);
                    task.put(AzureConstants.StrengthTask.BLAH_ID, blahId);
                    task.put(AzureConstants.StrengthTask.GROUP_ID, groupId);

                    // enqueue
                    CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
                    strengthTaskQueue.addMessage(message);
                    System.out.println("done");
                } else {
                    System.out.println("inactive, passed");
                }
            }
            cursor.close();
        }

        System.out.println(servicePrefix + "finish blah scanning.\n");
    }

    private void scanUsers() throws StorageException {
        System.out.println(servicePrefix + "start scanning users...");

        // scan by group
        for (String gid : groupNames.keySet()) {
            if (TEST_ONLY_TECH && !gid.equals("522ccb78e4b0a35dadfcf73f")) continue;

            String groupPrefix = "[" + groupNames.get(gid) + "] ";

            // get user-group info
            // only scan user whose "next check time" as passed
            List<BasicDBObject> orList = new ArrayList<>();
            orList.add(new BasicDBObject(DBConstants.UserGroupInfo.NEXT_CHECK_TIME, new BasicDBObject("$lt", new Date())));
            orList.add(new BasicDBObject(DBConstants.UserGroupInfo.NEXT_CHECK_TIME, new BasicDBObject("$exists", false)));
            BasicDBObject query = new BasicDBObject("$or", orList);
            query.append(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(gid));

            Cursor cursor = userGroupInfoCol.find(query);

            while (cursor.hasNext()) {
                BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
                String userId = userGroupInfo.get(DBConstants.UserGroupInfo.USER_ID).toString();
                String groupId = userGroupInfo.get(DBConstants.UserGroupInfo.GROUP_ID).toString();

                System.out.print(servicePrefix + groupPrefix + "check user <" + userId + "> in group '" + groupNames.get(groupId) + "'... ");

                if (userIsActive(userGroupInfo)) {
                    System.out.print("active, producing task... ");


                    // produce re-compute strength task
                    BasicDBObject task = new BasicDBObject();
                    task.put(AzureConstants.StrengthTask.TYPE, AzureConstants.StrengthTask.COMPUTE_USER_STRENGTH);
                    task.put(AzureConstants.StrengthTask.USER_ID, userId);
                    task.put(AzureConstants.StrengthTask.GROUP_ID, groupId);

                    // enqueue
                    CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
                    strengthTaskQueue.addMessage(message);
                    System.out.println("done");
                } else {
                    System.out.println("inactive, passed");
                }
            }
            cursor.close();
        }
        System.out.println(servicePrefix + "finish user scanning.\n");
    }

    private boolean blahIsActive(BasicDBObject blahInfo) {
        // get activity stats
        RecentBlahActivity stats = new RecentBlahActivity(blahInfo);

        // determine next check time based on activity and last update time
        updateBlahNextCheckTime(stats);

        long score = stats.opens + stats.comments * 5 + stats.upvotes * 10
                + stats.downvotes * 10 + stats.commentUpvotes * 5 + stats.commentDownvotes * 5;
        if (score >= 20) {
            // re-compute strength
            // remove new activity from infodb.blahInfo collection
            removeRecentBlahActivity(stats);
            return true;
        }
        else {
            return false;
        }
    }

    private boolean userIsActive(BasicDBObject userGroupInfo) {
        // get activity stats
        RecentUserActivity stats = new RecentUserActivity(userGroupInfo);

        // determine next check time based on activity and last update time
        updateUserNextCheckTime(stats);

        long score = stats.opens + stats.comments * 5 + stats.upvotes * 10
                + stats.downvotes * 10 + stats.commentUpvotes * 5 + stats.commentDownvotes * 5;
        if (score >= 20) {
            // re-compute strength
            // remove new activity from infodb.userGroupInfo collection
            removeRecentUserActivity(stats);
            return true;
        }
        else {
            return false;
        }
    }

    private class RecentBlahActivity {
        String blahId;
        Date lastUpdate;

        long views;
        long opens;
        long comments;
        long upvotes;
        long downvotes;
        long commentUpvotes;
        long commentDownvotes;

        private RecentBlahActivity(BasicDBObject blahInfo) {
            blahId = blahInfo.getString(DBConstants.BlahInfo.ID);
            lastUpdate = blahInfo.getDate(DBConstants.BlahInfo.STRENGTH_UPDATE_TIME, new Date(0L));

            BasicDBObject newActivity = (BasicDBObject) blahInfo.get(DBConstants.BlahInfo.NEW_ACTIVITY);
            if (newActivity != null) {
                views = newActivity.getInt(DBConstants.BlahInfo.NEW_VIEWS, 0);
                opens = newActivity.getInt(DBConstants.BlahInfo.NEW_OPENS, 0);
                comments = newActivity.getInt(DBConstants.BlahInfo.NEW_COMMENTS, 0);
                upvotes = newActivity.getInt(DBConstants.BlahInfo.NEW_UPVOTES, 0);
                downvotes = newActivity.getInt(DBConstants.BlahInfo.NEW_DOWNVOTES, 0);
                commentUpvotes = newActivity.getInt(DBConstants.BlahInfo.NEW_COMMENT_UPVOTES, 0);
                commentDownvotes = newActivity.getInt(DBConstants.BlahInfo.NEW_COMMENT_DOWNVOTES, 0);
            }
        }
    }

    private class RecentUserActivity {
        String userId;
        String groupId;
        Date lastUpdate;

        long views;
        long opens;
        long comments;
        long upvotes;
        long downvotes;
        long commentUpvotes;
        long commentDownvotes;

        private RecentUserActivity(BasicDBObject userGroupInfo) {
            userId = userGroupInfo.get(DBConstants.UserGroupInfo.USER_ID).toString();
            groupId = userGroupInfo.get(DBConstants.UserGroupInfo.GROUP_ID).toString();
            lastUpdate = userGroupInfo.getDate(DBConstants.UserGroupInfo.STRENGTH_UPDATE_TIME, new Date(0L));

            BasicDBObject newActivity = (BasicDBObject) userGroupInfo.get(DBConstants.UserGroupInfo.NEW_ACTIVITY);
            if (newActivity != null) {
                views = newActivity.getInt(DBConstants.UserGroupInfo.NEW_VIEWS, 0);
                opens = newActivity.getInt(DBConstants.UserGroupInfo.NEW_OPENS, 0);
                comments = newActivity.getInt(DBConstants.UserGroupInfo.NEW_COMMENTS, 0);
                upvotes = newActivity.getInt(DBConstants.UserGroupInfo.NEW_UPVOTES, 0);
                downvotes = newActivity.getInt(DBConstants.UserGroupInfo.NEW_DOWNVOTES, 0);
                commentUpvotes = newActivity.getInt(DBConstants.UserGroupInfo.NEW_COMMENT_UPVOTES, 0);
                commentDownvotes = newActivity.getInt(DBConstants.UserGroupInfo.NEW_COMMENT_DOWNVOTES, 0);
            }
        }
    }

    private void updateBlahNextCheckTime(RecentBlahActivity stats) {

        // hopefully in the future MongoDB can support java.time objects
        LocalDateTime lastUpdate = LocalDateTime.ofInstant(stats.lastUpdate.toInstant(), ZoneId.systemDefault());
        long hoursPassedSinceUpdate = ChronoUnit.HOURS.between(lastUpdate, LocalDateTime.now());
        double openPerHour = stats.opens / (double)hoursPassedSinceUpdate;

        // if get less than 1 open per hour on average, check again in 3 days
        // otherwise set to now, so it will be checked in next scan (no matter how frequently the scan is)
        LocalDateTime nextCheckTime;
        if (openPerHour < 0) {
            nextCheckTime = LocalDateTime.now().plusDays(3);
        }
        else {
            nextCheckTime = LocalDateTime.now();
        }
        // update database
        Date nextCheckTimeDate = Date.from(nextCheckTime.atZone(ZoneId.systemDefault()).toInstant());

        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.ID, new ObjectId(stats.blahId));
        BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConstants.BlahInfo.NEXT_CHECK_TIME, nextCheckTimeDate));

        blahInfoCol.update(query, setter);
    }

    private void updateUserNextCheckTime(RecentUserActivity stats) {
        // hopefully in the future MongoDB can support java.time objects
        LocalDateTime lastUpdate = LocalDateTime.ofInstant(stats.lastUpdate.toInstant(), ZoneId.systemDefault());
        long hoursPassedSinceUpdate = ChronoUnit.HOURS.between(lastUpdate, LocalDateTime.now());
        double openPerHour = stats.opens / (double)hoursPassedSinceUpdate;

        // if get less than 1 open per hour on average, check again in 3 days
        // otherwise set to now, so it will be checked in next scan (no matter how frequently the scan is)
        LocalDateTime nextCheckTime;
        if (openPerHour < 0) {
            nextCheckTime = LocalDateTime.now().plusDays(3);
        }
        else {
            nextCheckTime = LocalDateTime.now();
        }
        // update database
        Date nextCheckTimeDate = Date.from(nextCheckTime.atZone(ZoneId.systemDefault()).toInstant());

        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, new ObjectId(stats.userId));
        query.append(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(stats.groupId));
        BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConstants.UserGroupInfo.NEXT_CHECK_TIME, nextCheckTimeDate));

        userGroupInfoCol.update(query, setter);
    }

    private void removeRecentBlahActivity(RecentBlahActivity stats) {
        // there may be even newer activity when we are checking the recent activities of the blah
        // so subtract the stats in database by the amount in our check
        BasicDBObject values = new BasicDBObject();
        String newActPrefix = DBConstants.BlahInfo.NEW_ACTIVITY+".";
        if (stats.views > 0) values.put(newActPrefix+DBConstants.BlahInfo.NEW_VIEWS, -stats.views);
        if (stats.opens > 0) values.put(newActPrefix+DBConstants.BlahInfo.NEW_OPENS, -stats.opens);
        if (stats.comments > 0) values.put(newActPrefix+DBConstants.BlahInfo.NEW_COMMENTS, -stats.comments);
        if (stats.upvotes > 0) values.put(newActPrefix+DBConstants.BlahInfo.NEW_UPVOTES, -stats.upvotes);
        if (stats.downvotes > 0) values.put(newActPrefix+DBConstants.BlahInfo.NEW_DOWNVOTES, -stats.downvotes);
        if (stats.commentUpvotes > 0) values.put(newActPrefix+DBConstants.BlahInfo.NEW_COMMENT_UPVOTES, -stats.commentUpvotes);
        if (stats.commentDownvotes > 0) values.put(newActPrefix+DBConstants.BlahInfo.NEW_COMMENT_DOWNVOTES, -stats.commentDownvotes);

        BasicDBObject inc = new BasicDBObject("$inc", values);
        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.ID, new ObjectId(stats.blahId));

        blahInfoCol.update(query, inc);
    }

    private void removeRecentUserActivity(RecentUserActivity stats) {
        // there may be even newer activity when we are checking the recent activities of the user
        // so subtract the stats in database by the amount in our check
        BasicDBObject values = new BasicDBObject();
        String newActPrefix = DBConstants.UserGroupInfo.NEW_ACTIVITY+".";
        if (stats.views > 0) values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_VIEWS, -stats.views);
        if (stats.opens > 0) values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_OPENS, -stats.opens);
        if (stats.comments > 0) values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_COMMENTS, -stats.comments);
        if (stats.upvotes > 0) values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_UPVOTES, -stats.upvotes);
        if (stats.downvotes > 0) values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_DOWNVOTES, -stats.downvotes);
        if (stats.commentUpvotes > 0) values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_COMMENT_UPVOTES, -stats.commentUpvotes);
        if (stats.commentDownvotes > 0) values.put(newActPrefix+DBConstants.UserGroupInfo.NEW_COMMENT_DOWNVOTES, -stats.commentDownvotes);

        BasicDBObject inc = new BasicDBObject("$inc", values);
        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, new ObjectId(stats.userId));
        query.append(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(stats.groupId));

        userGroupInfoCol.update(query, inc);
    }
}
