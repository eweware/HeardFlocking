package com.eweware.heardflocking.strength;

import com.eweware.heardflocking.ServiceProperties;
import com.eweware.heardflocking.base.AzureConst;
import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.base.HeardAzure;
import com.eweware.heardflocking.base.HeardDB;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 10/22/14.
 */
public class StrengthMonitor extends TimerTask {

    private String servicePrefix = "[StrengthMonitor] ";

    public static void execute(HeardDB db, HeardAzure azure) {
        // TODO distribute group scanning evenly in a period of time?
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set period
        System.out.println("[StrengthMonitor] start running, period=" + PERIOD_MINUTES + " (minutes), time : "  + new Date());

        timer.schedule(new StrengthMonitor(db, azure, cal.getTime(), PERIOD_MINUTES), cal.getTime(), TimeUnit.MINUTES.toMillis(PERIOD_MINUTES));
    }

    public StrengthMonitor(HeardDB db, HeardAzure azure, Date startTime, int periodMinutes) {
        this.db = db;
        this.azure = azure;
        this.startTime = startTime;
        this.periodMinutes = periodMinutes;
        getGroups();
    }

    private HeardDB db;
    private HeardAzure azure;

    private HashMap<String, String> groupNames;

    private final Date startTime;
    private int periodMinutes;

    private static final int PERIOD_MINUTES = ServiceProperties.StrengthMonitor.PERIOD_MINUTES;
    private final int RECENT_BLAH_MONTHS = ServiceProperties.StrengthMonitor.RECENT_BLAH_MONTHS;

    // weights for check whether blah is active
    private final double bwV = ServiceProperties.StrengthMonitor.BLAH_WEIGHT_VIEW;
    private final double bwO = ServiceProperties.StrengthMonitor.BLAH_WEIGHT_OPEN;
    private final double bwC = ServiceProperties.StrengthMonitor.BLAH_WEIGHT_COMMENT;
    private final double bwP = ServiceProperties.StrengthMonitor.BLAH_WEIGHT_UPVOTES;
    private final double bwN = ServiceProperties.StrengthMonitor.BLAH_WEIGHT_DOWNVOTES;
    private final double bwCP = ServiceProperties.StrengthMonitor.BLAH_WEIGHT_COMMENT_UPVOTES;
    private final double bwCN = ServiceProperties.StrengthMonitor.BLAH_WEIGHT_COMMENT_DOWNVOTES;

    private final double delayBlahCheckThreshold = ServiceProperties.StrengthMonitor.DELAY_BLAH_CHECK_THRESHOLD;
    private final int delayBlahCheckHours = ServiceProperties.StrengthMonitor.DELAY_BLAH_CHECK_HOURS;
    private final double blahActiveThreshold = ServiceProperties.StrengthMonitor.BLAH_ACTIVE_THRESHOLD;

    // weights for check whether user is active
    private final double uwV = ServiceProperties.StrengthMonitor.USER_WEIGHT_VIEW;
    private final double uwO = ServiceProperties.StrengthMonitor.USER_WEIGHT_OPEN;
    private final double uwC = ServiceProperties.StrengthMonitor.USER_WEIGHT_COMMENT;
    private final double uwP = ServiceProperties.StrengthMonitor.USER_WEIGHT_UPVOTES;
    private final double uwN = ServiceProperties.StrengthMonitor.USER_WEIGHT_DOWNVOTES;
    private final double uwCP = ServiceProperties.StrengthMonitor.USER_WEIGHT_COMMENT_UPVOTES;
    private final double uwCN = ServiceProperties.StrengthMonitor.USER_WEIGHT_COMMENT_DOWNVOTES;

    private final double delayUserCheckThreshold = ServiceProperties.StrengthMonitor.DELAY_USER_CHECK_THRESHOLD;
    private final int delayUserCheckHours = ServiceProperties.StrengthMonitor.DELAY_USER_CHECK_HOURS;
    private final double userActiveThreshold = ServiceProperties.StrengthMonitor.USER_ACTIVE_THRESHOLD;

    private final boolean TEST_ONLY_TECH = ServiceProperties.TEST_ONLY_TECH;

    @Override
    public void run() {
        try {

            scanBlahs();
            scanUsers();

            printFinishInfo();
        }
        catch (Exception e) {
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

    private String getCurrentGeneration(String groupId) {
        BasicDBObject query = new BasicDBObject(DBConst.Groups.ID, new ObjectId(groupId));
        BasicDBObject group = (BasicDBObject) db.getGroupsCol().findOne(query);
        return group.getString(DBConst.Groups.CURRENT_GENERATION, null);
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

            String generationId = getCurrentGeneration(groupId);

            // if no generation available, skip scan
            if (generationId == null) {
                System.out.println(servicePrefix + groupPrefix + "no available generation, blah scan is skipped");
                return;
            }

            DBCursor cursor = getBlahs(groupId, earliestRelevantDate);

            while (cursor.hasNext()) {
                BasicDBObject blahInfo = (BasicDBObject) cursor.next();
                String blahId = blahInfo.getString(DBConst.BlahInfo.ID);

                System.out.print(servicePrefix + groupPrefix + "[blah] " + blahId + " ... ");

                if (blahIsActive(blahInfo)) {
                    System.out.print("active, producing task... ");
                    produceBlahStrengthTask(groupId, blahId, generationId);
                } else {
                    System.out.println("inactive, passed");
                }
            }
            cursor.close();
        }

        System.out.println(servicePrefix + "blah scanning finished\n");
    }

    private DBCursor getBlahs(String groupId, Date earliestRelevantDate) {
        // get all relevant blah info in this group
        BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.GROUP_ID, new ObjectId(groupId));
        query.put(DBConst.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate));

        // "relevant" also means the "next check time" for the blah has passed
        List<BasicDBObject> orList = new ArrayList<>();
        orList.add(new BasicDBObject(DBConst.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$lt", new Date())));
        orList.add(new BasicDBObject(DBConst.BlahInfo.NEXT_CHECK_TIME, new BasicDBObject("$exists", false)));
        query.put("$or", orList);

        return db.getBlahInfoCol().find(query);
    }

    private void produceBlahStrengthTask(String groupId, String blahId, String generationId) throws StorageException {

        // produce re-compute strength task
        BasicDBObject task = new BasicDBObject();
        task.put(AzureConst.StrengthTask.TYPE, AzureConst.StrengthTask.COMPUTE_BLAH_STRENGTH);
        task.put(AzureConst.StrengthTask.BLAH_ID, blahId);
        task.put(AzureConst.StrengthTask.GROUP_ID, groupId);
        task.put(AzureConst.StrengthTask.GENERATION_ID, generationId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        azure.getStrengthTaskQueue().addMessage(message);
        System.out.println("done");
    }

    private void scanUsers() throws StorageException {
        System.out.println(servicePrefix + "start scanning users...");

        // scan by group
        for (String groupId : groupNames.keySet()) {
            if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

            String groupPrefix = "[" + groupNames.get(groupId) + "] ";

            String generationId = getCurrentGeneration(groupId);

            // if no generation available, skip scan
            if (generationId == null) {
                System.out.println(servicePrefix + groupPrefix + "no available generation, user scan is skipped");
                continue;
            }

            DBCursor cursor = getUsers(groupId);

            while (cursor.hasNext()) {
                BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
                String userId = userGroupInfo.get(DBConst.UserGroupInfo.USER_ID).toString();

                System.out.print(servicePrefix + groupPrefix + "[user] " + userId + "... ");

                if (userIsActive(userGroupInfo)) {
                    System.out.print("active, producing task... ");
                    produceUserStrengthTask(groupId, userId, generationId);
                } else {
                    System.out.println("inactive, passed");
                }
            }
            cursor.close();
        }
        System.out.println(servicePrefix + "user scanning finished\n");
    }

    private DBCursor getUsers(String groupId) {
        // get user-group info
        // only scan user whose "next check time" as passed
        List<BasicDBObject> orList = new ArrayList<>();
        orList.add(new BasicDBObject(DBConst.UserGroupInfo.NEXT_CHECK_TIME, new BasicDBObject("$lt", new Date())));
        orList.add(new BasicDBObject(DBConst.UserGroupInfo.NEXT_CHECK_TIME, new BasicDBObject("$exists", false)));
        BasicDBObject query = new BasicDBObject("$or", orList);
        query.append(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(groupId));

        return db.getUserGroupInfoCol().find(query);
    }

    private void produceUserStrengthTask(String groupId, String userId, String generationId) throws StorageException {
        // produce re-compute strength task
        BasicDBObject task = new BasicDBObject();
        task.put(AzureConst.StrengthTask.TYPE, AzureConst.StrengthTask.COMPUTE_USER_STRENGTH);
        task.put(AzureConst.StrengthTask.USER_ID, userId);
        task.put(AzureConst.StrengthTask.GROUP_ID, groupId);
        task.put(AzureConst.StrengthTask.GENERATION_ID, generationId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        azure.getStrengthTaskQueue().addMessage(message);
        System.out.println("done");
    }

    private boolean blahIsActive(BasicDBObject blahInfo) {
        // get activity stats
        RecentBlahActivity stats = new RecentBlahActivity(blahInfo);

        double score = computeBlahActivityScore(stats);

        // determine next check time based on activity and last update time
        updateBlahNextCheckTime(stats, score);

        if (score >= blahActiveThreshold) {
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

        double score = computeUserActivityScore(stats);

        // determine next check time based on activity and last update time
        updateUserNextCheckTime(stats, score);

        if (score >= userActiveThreshold) {
            // re-compute strength
            // remove new activity from infodb.userGroupInfo collection
            removeRecentUserActivity(stats);
            return true;
        }
        else {
            return false;
        }
    }

    private double computeBlahActivityScore(RecentBlahActivity stats) {
        double score = stats.views * bwV
                + stats.opens * bwO
                + stats.comments * bwC
                + stats.upvotes * bwP
                + stats.downvotes * bwN
                + stats.commentUpvotes * bwCP
                + stats.commentDownvotes * bwCN;
        return score;
    }

    private double computeUserActivityScore(RecentUserActivity stats) {
        double score = stats.views * uwV
                + stats.opens * uwO
                + stats.comments * uwC
                + stats.upvotes * uwP
                + stats.downvotes * uwN
                + stats.commentUpvotes * uwCP
                + stats.commentDownvotes * uwCN;
        return score;
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
            blahId = blahInfo.getString(DBConst.BlahInfo.ID);
            lastUpdate = blahInfo.getDate(DBConst.BlahInfo.STRENGTH_UPDATE_TIME, new Date(0L));

            BasicDBObject newActivity = (BasicDBObject) blahInfo.get(DBConst.BlahInfo.NEW_ACTIVITY);
            if (newActivity != null) {
                views = newActivity.getInt(DBConst.BlahInfo.NEW_VIEWS, 0);
                opens = newActivity.getInt(DBConst.BlahInfo.NEW_OPENS, 0);
                comments = newActivity.getInt(DBConst.BlahInfo.NEW_COMMENTS, 0);
                upvotes = newActivity.getInt(DBConst.BlahInfo.NEW_UPVOTES, 0);
                downvotes = newActivity.getInt(DBConst.BlahInfo.NEW_DOWNVOTES, 0);
                commentUpvotes = newActivity.getInt(DBConst.BlahInfo.NEW_COMMENT_UPVOTES, 0);
                commentDownvotes = newActivity.getInt(DBConst.BlahInfo.NEW_COMMENT_DOWNVOTES, 0);
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
            userId = userGroupInfo.get(DBConst.UserGroupInfo.USER_ID).toString();
            groupId = userGroupInfo.get(DBConst.UserGroupInfo.GROUP_ID).toString();
            lastUpdate = userGroupInfo.getDate(DBConst.UserGroupInfo.STRENGTH_UPDATE_TIME, new Date(0L));

            BasicDBObject newActivity = (BasicDBObject) userGroupInfo.get(DBConst.UserGroupInfo.NEW_ACTIVITY);
            if (newActivity != null) {
                views = newActivity.getInt(DBConst.UserGroupInfo.NEW_VIEWS, 0);
                opens = newActivity.getInt(DBConst.UserGroupInfo.NEW_OPENS, 0);
                comments = newActivity.getInt(DBConst.UserGroupInfo.NEW_COMMENTS, 0);
                upvotes = newActivity.getInt(DBConst.UserGroupInfo.NEW_UPVOTES, 0);
                downvotes = newActivity.getInt(DBConst.UserGroupInfo.NEW_DOWNVOTES, 0);
                commentUpvotes = newActivity.getInt(DBConst.UserGroupInfo.NEW_COMMENT_UPVOTES, 0);
                commentDownvotes = newActivity.getInt(DBConst.UserGroupInfo.NEW_COMMENT_DOWNVOTES, 0);
            }
        }
    }

    private void updateBlahNextCheckTime(RecentBlahActivity stats, double score) {

        // hopefully in the future MongoDB can support java.time objects
        LocalDateTime lastUpdate = LocalDateTime.ofInstant(stats.lastUpdate.toInstant(), ZoneId.systemDefault());
        long hoursPassedSinceUpdate = ChronoUnit.HOURS.between(lastUpdate, LocalDateTime.now());
        double scorePerHour = score / (double)hoursPassedSinceUpdate;

        // if get less than 1 open per hour on average, check again in 3 days
        // otherwise set to now, so it will be checked in next scan (no matter how frequently the scan is)
        LocalDateTime nextCheckTime;
        if (scorePerHour < delayBlahCheckThreshold) {
            nextCheckTime = LocalDateTime.now().plusHours(delayBlahCheckHours);
        }
        else {
            nextCheckTime = LocalDateTime.now();
        }
        // update database
        Date nextCheckTimeDate = Date.from(nextCheckTime.atZone(ZoneId.systemDefault()).toInstant());

        BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.ID, new ObjectId(stats.blahId));
        BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConst.BlahInfo.NEXT_CHECK_TIME, nextCheckTimeDate));

        db.getBlahInfoCol().update(query, setter);
    }

    private void updateUserNextCheckTime(RecentUserActivity stats, double score) {
        // hopefully in the future MongoDB can support java.time objects
        LocalDateTime lastUpdate = LocalDateTime.ofInstant(stats.lastUpdate.toInstant(), ZoneId.systemDefault());
        long hoursPassedSinceUpdate = ChronoUnit.HOURS.between(lastUpdate, LocalDateTime.now());
        double openPerHour = score / (double)hoursPassedSinceUpdate;

        // if get less than 1 open per hour on average, check again in 3 days
        // otherwise set to now, so it will be checked in next scan (no matter how frequently the scan is)
        LocalDateTime nextCheckTime;
        if (openPerHour < delayUserCheckThreshold) {
            nextCheckTime = LocalDateTime.now().plusHours(delayUserCheckHours);
        }
        else {
            nextCheckTime = LocalDateTime.now();
        }
        // update database
        Date nextCheckTimeDate = Date.from(nextCheckTime.atZone(ZoneId.systemDefault()).toInstant());

        BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.USER_ID, new ObjectId(stats.userId));
        query.append(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(stats.groupId));
        BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConst.UserGroupInfo.NEXT_CHECK_TIME, nextCheckTimeDate));

        db.getUserGroupInfoCol().update(query, setter);
    }

    private void removeRecentBlahActivity(RecentBlahActivity stats) {
        // there may be even newer activity when we are checking the recent activities of the blah
        // so subtract the stats in database by the amount in our check
        BasicDBObject values = new BasicDBObject();
        String newActPrefix = DBConst.BlahInfo.NEW_ACTIVITY+".";
        if (stats.views > 0) values.put(newActPrefix+ DBConst.BlahInfo.NEW_VIEWS, -stats.views);
        if (stats.opens > 0) values.put(newActPrefix+ DBConst.BlahInfo.NEW_OPENS, -stats.opens);
        if (stats.comments > 0) values.put(newActPrefix+ DBConst.BlahInfo.NEW_COMMENTS, -stats.comments);
        if (stats.upvotes > 0) values.put(newActPrefix+ DBConst.BlahInfo.NEW_UPVOTES, -stats.upvotes);
        if (stats.downvotes > 0) values.put(newActPrefix+ DBConst.BlahInfo.NEW_DOWNVOTES, -stats.downvotes);
        if (stats.commentUpvotes > 0) values.put(newActPrefix+ DBConst.BlahInfo.NEW_COMMENT_UPVOTES, -stats.commentUpvotes);
        if (stats.commentDownvotes > 0) values.put(newActPrefix+ DBConst.BlahInfo.NEW_COMMENT_DOWNVOTES, -stats.commentDownvotes);

        BasicDBObject inc = new BasicDBObject("$inc", values);
        BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.ID, new ObjectId(stats.blahId));

        db.getBlahInfoCol().update(query, inc);
    }

    private void removeRecentUserActivity(RecentUserActivity stats) {
        // there may be even newer activity when we are checking the recent activities of the user
        // so subtract the stats in database by the amount in our check
        BasicDBObject values = new BasicDBObject();
        String newActPrefix = DBConst.UserGroupInfo.NEW_ACTIVITY+".";
        if (stats.views > 0) values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_VIEWS, -stats.views);
        if (stats.opens > 0) values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_OPENS, -stats.opens);
        if (stats.comments > 0) values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_COMMENTS, -stats.comments);
        if (stats.upvotes > 0) values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_UPVOTES, -stats.upvotes);
        if (stats.downvotes > 0) values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_DOWNVOTES, -stats.downvotes);
        if (stats.commentUpvotes > 0) values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_COMMENT_UPVOTES, -stats.commentUpvotes);
        if (stats.commentDownvotes > 0) values.put(newActPrefix+ DBConst.UserGroupInfo.NEW_COMMENT_DOWNVOTES, -stats.commentDownvotes);

        BasicDBObject inc = new BasicDBObject("$inc", values);
        BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.USER_ID, new ObjectId(stats.userId));
        query.append(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(stats.groupId));

        db.getUserGroupInfoCol().update(query, inc);
    }

    private void printFinishInfo() {
        Calendar nextTime = Calendar.getInstance();
        nextTime.setTime(startTime);
        nextTime.add(Calendar.MINUTE, periodMinutes);
        System.out.println(servicePrefix + "next scan in less than " + periodMinutes + " minutes at time : " + nextTime.getTime());
    }
}
