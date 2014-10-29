package com.eweware.heardflocking.strength;

import com.eweware.heardflocking.AzureConstants;
import com.eweware.heardflocking.DBConstants;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;
import com.mongodb.*;
import com.mongodb.util.JSON;

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

    private CloudQueueClient queueClient;
    private CloudQueue strengthTaskQueue;

    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;

    private DBCollection groupsCol;
    private DBCollection blahInfoCol;
    private DBCollection userGroupInfoCol;

    private HashMap<String, String> groupNames;

    public static void main(String[] args) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set time to run
//        cal.set(Calendar.HOUR_OF_DAY, 20);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // set period
        int periodHours = 24;

        System.out.println("StrengthMonitor set to run once for every " + periodHours + " hours, starting at "  + cal.getTime().toString());

        timer.schedule(new StrengthMonitor(), cal.getTime(), TimeUnit.HOURS.toMillis(periodHours));
    }

    @Override
    public void run() {
        try {
            initializeQueue();
            initializeMongoDB();

            scanBlahs();
            //scanUsers();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeQueue() throws Exception {
        System.out.print("Initializing Azure Storage Queue service... ");

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
        System.out.print("Initializing MongoDB connection... ");

        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DEV_DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");

        System.out.println("done");

        // get group names
        groupNames = new HashMap<String, String>();
        DBCursor cursor = groupsCol.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private void scanBlahs() throws StorageException {
        System.out.println("### Start scanning blahs...");
        // only look at blahs created within certain number of months
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -24);
        Date earliestRelevantDate = cal.getTime();

        // get all relevant blah info
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate));
        BasicDBObject or = new BasicDBObject(DBConstants.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$lt", new Date()));
        or.append(DBConstants.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$exists", false));
        query.put("$or", or);
        Cursor cursor = blahInfoCol.find(query);

        while (cursor.hasNext()) {
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            String blahId = blahInfo.getString(DBConstants.BlahInfo.ID);

            System.out.print("Checking blah <" + blahId + "> ... ");

            if (blahIsActive(blahInfo)) {
                System.out.print("active, producing task... ");
                String groupId = blahInfo.getString(DBConstants.BlahInfo.GROUP_ID);

                // produce re-compute strength task
                BasicDBObject task = new BasicDBObject();
                task.put(AzureConstants.StrengthTask.TYPE, AzureConstants.StrengthTask.COMPUTE_BLAH_STRENGTH);
                task.put(AzureConstants.StrengthTask.BLAH_ID, blahId);
                task.put(AzureConstants.StrengthTask.GROUP_ID, groupId);

                // enqueue
                CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
                strengthTaskQueue.addMessage(message);
                System.out.println("done");
            }
            else {
                System.out.println("inactive, passed");
            }
        }
        cursor.close();
        System.out.println("### Finish blah scanning.\n");
    }

    private void scanUsers() throws StorageException {
        System.out.println("### Start scanning users...");
        // get user-group info
        BasicDBObject or = new BasicDBObject(DBConstants.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$lt", new Date()));
        or.append(DBConstants.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$exists", false));
        BasicDBObject query = new BasicDBObject("$or", or);

        Cursor cursor = userGroupInfoCol.find(query);

        while (cursor.hasNext()) {
            BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
            String userId = (String) userGroupInfo.get(DBConstants.UserGroupInfo.USER_ID);

            System.out.print("Checking user <" + userId + "> ... ");

            if (userIsActive(userGroupInfo)) {
                System.out.print("active, producing task... ");
                String groupId = (String) userGroupInfo.get(DBConstants.UserGroupInfo.GROUP_ID);

                // produce re-compute strength task
                BasicDBObject task = new BasicDBObject();
                task.put(AzureConstants.StrengthTask.TYPE, AzureConstants.StrengthTask.COMPUTE_USER_STRENGTH);
                task.put(AzureConstants.StrengthTask.USER_ID, userId);
                task.put(AzureConstants.StrengthTask.GROUP_ID, groupId);

                // enqueue
                CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
                strengthTaskQueue.addMessage(message);
                System.out.println("done");
            }
            else {
                System.out.println("inactive, passed");
            }
        }
        cursor.close();
        System.out.println("### Finish user scanning.\n");
    }

    private boolean blahIsActive(BasicDBObject blahInfo) {
        // get activity stats
        RecentBlahActivity stats = new RecentBlahActivity(blahInfo);

        updateBlahNextCheckTime(stats);

        if (stats.comments + stats.upvotes + stats.downvotes >= 3) {
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

        updateUserNextCheckTime(stats);

        if (stats.comments + stats.upvotes + stats.downvotes >= 5) {
            // re-compute strength
            // remove new activity from infodb.usergroupInfo collection
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

        int views;
        int opens;
        int comments;
        int upvotes;
        int downvotes;
        int commentUpvotes;
        int commentDownvotes;

        private RecentBlahActivity(BasicDBObject blahInfo) {
            blahId = blahInfo.getString(DBConstants.BlahInfo.ID);
            lastUpdate = blahInfo.getDate(DBConstants.BlahInfo.STRENGTH_UPDATE_TIME, new Date(0L));

            views = blahInfo.getInt(DBConstants.BlahInfo.NEW_VIEWS, 0);
            opens = blahInfo.getInt(DBConstants.BlahInfo.NEW_OPENS, 0);
            comments = blahInfo.getInt(DBConstants.BlahInfo.NEW_COMMENTS, 0);
            upvotes= blahInfo.getInt(DBConstants.BlahInfo.NEW_UPVOTES, 0);
            downvotes = blahInfo.getInt(DBConstants.BlahInfo.NEW_DOWNVOTES, 0);
            commentUpvotes = blahInfo.getInt(DBConstants.BlahInfo.NEW_COMMENT_UPVOTES, 0);
            commentDownvotes = blahInfo.getInt(DBConstants.BlahInfo.NEW_COMMENT_DOWNVOTES, 0);
        }
    }

    private class RecentUserActivity {
        String userId;
        String groupId;
        Date lastUpdate;

        int views;
        int opens;
        int comments;
        int upvotes;
        int downvotes;
        int commentUpvotes;
        int commentDownvotes;

        private RecentUserActivity(BasicDBObject userGroupInfo) {
            userId = userGroupInfo.getString(DBConstants.UserGroupInfo.USER_ID);
            groupId = userGroupInfo.getString(DBConstants.UserGroupInfo.GROUP_ID);
            lastUpdate = userGroupInfo.getDate(DBConstants.UserGroupInfo.STRENGTH_UPDATE_TIME, new Date(0L));

            views = userGroupInfo.getInt(DBConstants.UserGroupInfo.NEW_VIEWS, 0);
            opens = userGroupInfo.getInt(DBConstants.UserGroupInfo.NEW_OPENS, 0);
            comments = userGroupInfo.getInt(DBConstants.UserGroupInfo.NEW_COMMENTS, 0);
            upvotes= userGroupInfo.getInt(DBConstants.UserGroupInfo.NEW_UPVOTES, 0);
            downvotes = userGroupInfo.getInt(DBConstants.UserGroupInfo.NEW_DOWNVOTES, 0);
            commentUpvotes = userGroupInfo.getInt(DBConstants.UserGroupInfo.NEW_COMMENT_UPVOTES, 0);
            commentDownvotes = userGroupInfo.getInt(DBConstants.UserGroupInfo.NEW_COMMENT_DOWNVOTES, 0);
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

        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.ID, stats.blahId);
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

        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, stats.userId);
        query.append(DBConstants.UserGroupInfo.GROUP_ID, stats.groupId);
        BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConstants.UserGroupInfo.NEXT_CHECK_TIME, nextCheckTimeDate));

        userGroupInfoCol.update(query, setter);
    }

    private void removeRecentBlahActivity(RecentBlahActivity stats) {
        // there may be even newer activity when we are checking the recent activities of the blah
        // so subtract the stats in database by the amount in our check
        BasicDBObject values = new BasicDBObject();
        if (stats.views > 0) values.put(DBConstants.BlahInfo.NEW_VIEWS, -stats.views);
        if (stats.opens > 0) values.put(DBConstants.BlahInfo.NEW_OPENS, -stats.opens);
        if (stats.comments > 0) values.put(DBConstants.BlahInfo.NEW_COMMENTS, -stats.comments);
        if (stats.upvotes > 0) values.put(DBConstants.BlahInfo.NEW_UPVOTES, -stats.upvotes);
        if (stats.downvotes > 0) values.put(DBConstants.BlahInfo.NEW_DOWNVOTES, -stats.downvotes);
        if (stats.commentUpvotes > 0) values.put(DBConstants.BlahInfo.NEW_COMMENT_UPVOTES, -stats.commentUpvotes);
        if (stats.commentDownvotes > 0) values.put(DBConstants.BlahInfo.NEW_COMMENT_DOWNVOTES, -stats.commentDownvotes);

        BasicDBObject inc = new BasicDBObject("$inc", values);
        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.ID, stats.blahId);

        blahInfoCol.update(query, inc);
    }

    private void removeRecentUserActivity(RecentUserActivity stats) {
        // there may be even newer activity when we are checking the recent activities of the user
        // so subtract the stats in database by the amount in our check
        BasicDBObject values = new BasicDBObject();
        if (stats.views > 0) values.put(DBConstants.UserGroupInfo.NEW_VIEWS, -stats.views);
        if (stats.opens > 0) values.put(DBConstants.UserGroupInfo.NEW_OPENS, -stats.opens);
        if (stats.comments > 0) values.put(DBConstants.UserGroupInfo.NEW_COMMENTS, -stats.comments);
        if (stats.upvotes > 0) values.put(DBConstants.UserGroupInfo.NEW_UPVOTES, -stats.upvotes);
        if (stats.downvotes > 0) values.put(DBConstants.UserGroupInfo.NEW_DOWNVOTES, -stats.downvotes);
        if (stats.commentUpvotes > 0) values.put(DBConstants.UserGroupInfo.NEW_COMMENT_UPVOTES, -stats.commentUpvotes);
        if (stats.commentDownvotes > 0) values.put(DBConstants.UserGroupInfo.NEW_COMMENT_DOWNVOTES, -stats.commentDownvotes);

        BasicDBObject inc = new BasicDBObject("$inc", values);
        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, stats.userId);
        query.append(DBConstants.UserGroupInfo.GROUP_ID, stats.groupId);

        userGroupInfoCol.update(query, inc);
    }
}
