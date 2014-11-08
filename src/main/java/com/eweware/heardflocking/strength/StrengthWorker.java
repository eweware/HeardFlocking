package com.eweware.heardflocking.strength;


import Jama.Matrix;
import com.eweware.heardflocking.base.AzureConst;
import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.ServiceProperties;
import com.eweware.heardflocking.base.HeardAzure;
import com.eweware.heardflocking.base.HeardDB;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * Created by weihan on 10/23/14.
 */
public class StrengthWorker {
    public StrengthWorker(HeardDB db, HeardAzure azure) {
        this.db = db;
        this.azure = azure;
        getGroups();
    }

    HeardDB db;
    HeardAzure azure;

    HashMap<String, String> groupNames;

    // each task is given this amount of time for processing before it become visible again in the queue
    final int QUEUE_VISIBLE_TIMEOUT_SECONDS = ServiceProperties.StrengthWorker.QUEUE_VISIBLE_TIMEOUT_SECONDS;
    final long NO_TASK_WAIT_MILLIS = ServiceProperties.StrengthWorker.NO_TASK_WAIT_MILLIS;

    // author's strength depends on his blahs' strength, only relevant blahs are taken into account
    // a blah is relevant if it was posted within this number of days in the past
    final int RECENT_BLAH_DAYS = ServiceProperties.StrengthWorker.RECENT_BLAH_DAYS;

    // weights for user-blah utility
    final double wV = ServiceProperties.StrengthWorker.WEIGHT_VIEW;
    final double wO = ServiceProperties.StrengthWorker.WEIGHT_OPEN;
    final double wC = ServiceProperties.StrengthWorker.WEIGHT_COMMENT;
    final double wP = ServiceProperties.StrengthWorker.WEIGHT_UPVOTES;
    final double wN = ServiceProperties.StrengthWorker.WEIGHT_DOWNVOTES;
    final double wCP = ServiceProperties.StrengthWorker.WEIGHT_COMMENT_UPVOTES;
    final double wCN = ServiceProperties.StrengthWorker.WEIGHT_COMMENT_DOWNVOTES;

    private String servicePrefix = "[StrengthWorker] ";

    public void execute() {
        try {
            System.out.println(servicePrefix + "start running");

            System.out.println();

            // continuously get task to work on
            while (true) {
                CloudQueueMessage message = azure.getStrengthTaskQueue().retrieveMessage(QUEUE_VISIBLE_TIMEOUT_SECONDS, null, new OperationContext());
                if (message != null) {
                    // Process the message within certain time, and then delete the message.
                    BasicDBObject task = (BasicDBObject) JSON.parse(message.getMessageContentAsString());
                    try {
                        processTask(task);
                        azure.getStrengthTaskQueue().deleteMessage(message);
                    }
                    catch (TaskException e) {
                        // there is something wrong about the task
                        if (e.type == TaskExceptionType.SKIP) {
                            System.out.println(e.getMessage() + ", task skipped");
                            azure.getStrengthTaskQueue().deleteMessage(message);
                        }
                        else if (e.type == TaskExceptionType.RECOMPUTE) {
                            System.out.println(e.getMessage() + ", task put back to queue");
                            azure.getStrengthTaskQueue().updateMessage(message, 0);
                        }
                        else
                            e.printStackTrace();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                else {
                    System.out.println(servicePrefix + "no task, sleep for " + NO_TASK_WAIT_MILLIS + " milliseconds");
                    Thread.sleep(NO_TASK_WAIT_MILLIS);
                }
            }
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

    private void processTask(BasicDBObject task) throws TaskException, StorageException {
        int taskType = (Integer) task.get(AzureConst.StrengthTask.TYPE);

        if (taskType == AzureConst.StrengthTask.COMPUTE_BLAH_STRENGTH) {
            String blahId = task.getString(AzureConst.StrengthTask.BLAH_ID);
            String groupId = task.getString(AzureConst.StrengthTask.GROUP_ID);
            String generationId = task.getString(AzureConst.StrengthTask.GENERATION_ID);

            System.out.print(servicePrefix + "[blah] ");

            HashMap<String, Double> cohortStrength = computeBlahStrength(blahId, groupId, generationId);
            updateBlahStrength(blahId, cohortStrength);
        }
        else if (taskType == AzureConst.StrengthTask.COMPUTE_USER_STRENGTH) {
            String userId = (String) task.get(AzureConst.StrengthTask.USER_ID);
            String groupId = (String) task.get(AzureConst.StrengthTask.GROUP_ID);
            String generationId = task.getString(AzureConst.StrengthTask.GENERATION_ID);

            System.out.print(servicePrefix + "[blah] ");

            HashMap<String, Double> cohortStrength = computeUserStrength(userId, groupId, generationId);
            updateUserStrength(userId, groupId, cohortStrength);
        }
        else if (taskType == AzureConst.StrengthTask.COMPUTE_ALL_STRENGTH) {
            String groupId = (String) task.get(AzureConst.StrengthTask.GROUP_ID);
            String generationId = task.getString(AzureConst.StrengthTask.GENERATION_ID);

            computeAndUpdateAllStrength(groupId, generationId);
        }
        else {
            throw new TaskException("Error : undefined task type : " + taskType, TaskExceptionType.SKIP);
        }
    }

    private enum TaskExceptionType {
        RECOMPUTE, SKIP
    }

    private class TaskException extends Exception {
        private TaskExceptionType type;
        private TaskException(String msg, TaskExceptionType type) {
            super(msg);
            this.type = type;
        }
    }

    private HashMap<String, Double> computeBlahStrength(String blahId, String groupId, String generationId) throws TaskException {
        System.out.print(blahId + " ...");

        // get cohort list for this generation
        List<ObjectId> cohortList = getCohortList(generationId);

        // build cohortId -> index map
        HashMap<String, Integer> cohortIdIndexMap = new HashMap<>();
        int c = 0;
        for (ObjectId cohortIdObj : cohortList)
            cohortIdIndexMap.put(cohortIdObj.toString(), c++);

        // get user-blah util vector for this blah
        Cursor cursor = blahAggregation(blahId);

        // declare final cohort-strength result in vector form
        double[] cohortStrengthVec;
        double defaultCohortStrength;

        // if the blah doesn't have any activity, strength is 0
        if (!cursor.hasNext()) {
            cohortStrengthVec = new double[cohortIdIndexMap.size()];
            defaultCohortStrength = 0;
        }
        else {
            // the aggregation is grouped by userId, for each user, compute utility and store
            // add userId and corresponding utility to two corresponding lists
            List<String> userIdList = new ArrayList<>();
            ArrayList<Double> userUtilList = new ArrayList<>();
            while (cursor.hasNext()) {
                BasicDBObject userBlah = (BasicDBObject) cursor.next();
                UserBlahInfo userBlahInfo = new UserBlahInfo(userBlah, blahId);
                userIdList.add(userBlahInfo.userId.toString());
                userUtilList.add(computeUtility(userBlahInfo));
            }

            // compute default cohort strength
            defaultCohortStrength = computeDefaultCohortStrength(userUtilList);

            // get user cohort cluster info
            // matrix[userIndex][cohortIndex] = 1 when the user is in the cohort, = 0 otherwise
            // there is data inconsistency between userBlahStats and userGroupInfo, delete those rows
            HashSet<Integer> deleteRow = new HashSet<>();
            double[][] userCohortMtx = getUserCohortInfo(userIdList, cohortIdIndexMap, groupId, generationId, deleteRow);

            // exclude rows affected by inconsistent data
            double[][] userCohortMtxValid;
            if (deleteRow.size() > 0) {
                userCohortMtxValid = new double[userIdList.size() - deleteRow.size()][cohortIdIndexMap.size()];
                int j = 0;
                for (int i = 0; i < userIdList.size(); i++) {
                    if (!deleteRow.contains(i)) {
                        userCohortMtxValid[j] = userCohortMtx[i];
                        j++;
                    }
                }
            }
            else
                userCohortMtxValid = userCohortMtx;

            // convert userUtilList to double[]
            // exclude rows affected by inconsistent data
            double[] userUtilVec = new double[userUtilList.size() - deleteRow.size()];
            int j = 0;
            for (int i = 0; i < userUtilList.size(); i++) {
                if (!deleteRow.contains(i)) {
                    userUtilVec[j] = userUtilList.get(i);
                    j++;
                }
            }

            // use userCohortMtx and userUtilVec to compute cohortStrengthVec
            cohortStrengthVec = nonNegativeLeastSquares(userCohortMtxValid, userUtilVec);

        }
        // build cohortIdStrengthMap
        HashMap<String, Double> cohortStrength = new HashMap<>();
        for (String cohortId : cohortIdIndexMap.keySet()) {
            int index = cohortIdIndexMap.get(cohortId);
            cohortStrength.put(cohortId, cohortStrengthVec[index]);
        }

        // add default cohort strength
        String defaultCohortId = getDefaultCohortId(generationId);
        cohortStrength.put(defaultCohortId, defaultCohortStrength);

        System.out.print("done");
        return cohortStrength;
    }

    private Cursor blahAggregation(String blahId) {
        // aggregate each user's activities for this blah
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject(DBConst.UserBlahStats.BLAH_ID, new ObjectId(blahId)));

        BasicDBObject groupFields = new BasicDBObject("_id", "$"+ DBConst.UserBlahStats.USER_ID);
        groupFields.append(DBConst.UserBlahStats.VIEWS, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.VIEWS));
        groupFields.append(DBConst.UserBlahStats.OPENS, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.OPENS));
        groupFields.append(DBConst.UserBlahStats.COMMENTS, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.COMMENTS));
        groupFields.append(DBConst.UserBlahStats.COMMENT_UPVOTES, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.COMMENT_UPVOTES));
        BasicDBObject groupBy = new BasicDBObject("$group", groupFields);

        List<DBObject> pipeline = Arrays.asList(match, groupBy);

        AggregationOptions aggregationOptions = AggregationOptions.builder()
//                .batchSize(100)
                .outputMode(AggregationOptions.OutputMode.CURSOR)
                .allowDiskUse(true)
                .build();

        return db.getUserBlahStatsCol().aggregate(pipeline, aggregationOptions);
    }

    private class UserBlahInfo {
        ObjectId blahId;
        ObjectId userId;

        long views;
        long opens;
        long comments;
        long upvotes;
        long downvotes;
        long commentUpvotes;
        long commentDownvotes;

        private UserBlahInfo(BasicDBObject userBlah, String blahId) {
            userId = (ObjectId) userBlah.get("_id");
            this.blahId = new ObjectId(blahId);

            views = userBlah.getLong(DBConst.UserBlahStats.VIEWS, 0L);
            opens = userBlah.getLong(DBConst.UserBlahStats.OPENS, 0L);
            comments = userBlah.getLong(DBConst.UserBlahStats.COMMENTS, 0L);
            upvotes = userBlah.getLong(DBConst.UserBlahStats.UPVOTES, 0L);
            downvotes = userBlah.getLong(DBConst.UserBlahStats.DOWNVOTES, 0L);
            commentUpvotes = userBlah.getLong(DBConst.UserBlahStats.COMMENT_UPVOTES, 0L);
            commentDownvotes = userBlah.getLong(DBConst.UserBlahStats.COMMENT_DOWNVOTES, 0L);
        }
    }

    private List<ObjectId> getCohortList(String generationId) throws TaskException {
        BasicDBObject query = new BasicDBObject(DBConst.GenerationInfo.ID, new ObjectId(generationId));
        BasicDBObject generationInfo = (BasicDBObject) db.getGenerationInfoCol().findOne(query);
        if (generationInfo == null) throw new TaskException("Error : generation info not found for ID : " + generationId, TaskExceptionType.SKIP);
        return (List<ObjectId>) generationInfo.get(DBConst.GenerationInfo.COHORT_LIST);
    }

    private double computeUtility(UserBlahInfo userBlahInfo) {
        // P > 0 upvote:    more people should see this
        // P < 0 downvode:  fewer people should see this

        // if a user view a blah but didn't open, that's a sign for negative utility
        double util = userBlahInfo.opens * wO +
                userBlahInfo.comments * wC +
                userBlahInfo.upvotes * wP  +
                userBlahInfo.downvotes * wN +
                userBlahInfo.commentUpvotes * wCP +
                userBlahInfo.commentDownvotes * wCN +
                Math.signum(userBlahInfo.views) * wV;
        return util;
    }

    private double[][] getUserCohortInfo(List<String> userIdList, HashMap<String, Integer> cohortIdIndexMap, String groupId, String generationId, HashSet<Integer> deleteRow) throws TaskException {

        double[][] userCohortMtx = new double[userIdList.size()][cohortIdIndexMap.size()];

        for (int u = 0; u < userIdList.size(); u++) {
            String userId = userIdList.get(u);
            BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.USER_ID, new ObjectId(userId));
            query.put(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(groupId));

            BasicDBObject userGroupInfo = (BasicDBObject) db.getUserGroupInfoCol().findOne(query);

            if (userGroupInfo == null) {
                // this happens because there are users who act on this group's blah but is not in that group
                // data inconsistent!
                deleteRow.add(u);
//                throw new TaskException("Error : user-group info not found for userID : " + userId + " groupId : " + groupId, TaskExceptionType.SKIP);
            }
            else {
                BasicDBObject cohortGenerations = (BasicDBObject) userGroupInfo.get(DBConst.UserGroupInfo.COHORT_GENERATIONS);
                List<ObjectId> userCohortIdObjList = (List<ObjectId>) cohortGenerations.get(generationId);
                for (ObjectId cohortId : userCohortIdObjList) {
                    userCohortMtx[u][cohortIdIndexMap.get(cohortId.toString())] = 1;
                }
            }
        }
        return userCohortMtx;
    }

    private double[] nonNegativeLeastSquares(double[][] userCohortMtx, double[] userUtilVec) throws TaskException {
        Matrix A = new Matrix(userCohortMtx);
        Matrix b = new Matrix(userUtilVec, userUtilVec.length);
        // non-negative least squares, based on JAMA
        Matrix x;
        try {
            x = NNLSSolver.solveNNLS(A, b);
        }
        catch (RuntimeException e) {
            throw new TaskException("Numeric computation error : " + e.getMessage(), TaskExceptionType.SKIP);
        }
        return x.getColumnPackedCopy();
    }

    private void updateBlahStrength(String blahId, HashMap<String, Double> cohortStrength) {
        System.out.print(" update database...");
        BasicDBObject values = new BasicDBObject();
        for (String cohortId : cohortStrength.keySet()) {
            values.put(DBConst.BlahInfo.STRENGTH + "." + cohortId, cohortStrength.get(cohortId));
        }
        values.put(DBConst.BlahInfo.STRENGTH_UPDATE_TIME, new Date());

        BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.ID, new ObjectId(blahId));
        BasicDBObject setter = new BasicDBObject("$set", values);
        db.getBlahInfoCol().update(query, setter);
        System.out.println("done");
    }

    private String getDefaultCohortId(String generationId) throws TaskException {
        BasicDBObject query = new BasicDBObject(DBConst.GenerationInfo.ID, new ObjectId(generationId));
        BasicDBObject generationInfo = (BasicDBObject) db.getGenerationInfoCol().findOne(query);
        if (generationInfo == null) throw new TaskException("Error : generation info not found for ID : " + generationId, TaskExceptionType.SKIP);
        return generationInfo.getObjectId(DBConst.GenerationInfo.DEFAULT_COHORT).toString();
    }

    private double computeDefaultCohortStrength(List<Double> userUtilList) {
        // take average
        double sum = 0;
        for (Double util : userUtilList) {
            sum += util;
        }
        return sum / userUtilList.size();
    }

    private HashMap<String, Double> computeUserStrength(String userId, String groupId, String generationId) {
        System.out.print(userId + " ...");

        // get cohort list for this group for current generation or next generation
        List<String> cohortIdList = getCohortIdList(generationId);

        // for all recent blahs authored by this user, get the blahs' cohort strength info
        // cohortId -> (blahId -> strength)
        HashMap<String, HashMap<String, Double>> blahStrengthInCohort = getBlahStrengthInCohort(userId, groupId, cohortIdList);

        // compute user's cohort-strength
        HashMap<String, Double> userCohortStrength = computeAvgBlahStrengthForAllCohort(blahStrengthInCohort);

        System.out.print("done");
        return userCohortStrength;
    }

    private List<String> getCohortIdList(String generationId) {
        // get this generation's cohort list
        BasicDBObject query = new BasicDBObject(DBConst.GenerationInfo.ID, new ObjectId(generationId));
        BasicDBObject generation = (BasicDBObject) db.getGenerationInfoCol().findOne(query);
        List<ObjectId> cohortList = (List<ObjectId>) generation.get(DBConst.GenerationInfo.COHORT_LIST);

        List<String> cohortIdList = new ArrayList<>();
        for (ObjectId cohortIdObj : cohortList) {
            cohortIdList.add(cohortIdObj.toString());
        }

        return cohortIdList;
    }

    private HashMap<String, HashMap<String, Double>> getBlahStrengthInCohort(String userId, String groupId, List<String> cohortIdList) {
        HashMap<String, HashMap<String, Double>> blahStrengthInCohort = new HashMap<>();
        // initialize
        // notice here only have cohortId for this generation
        for (String cohortId : cohortIdList)
            blahStrengthInCohort.put(cohortId, new HashMap<>());

        // get blahInfo authored by this user in this group
        BasicDBObject query = new BasicDBObject();
        query.put(DBConst.BlahInfo.AUTHOR_ID, new ObjectId(userId));
        query.put(DBConst.BlahInfo.GROUP_ID, new ObjectId(groupId));

        // only consider blahs authored by the user in the recent certain number of months
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -RECENT_BLAH_DAYS);
        Date earliestRelevantDate = cal.getTime();
        query.put(DBConst.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate));

        Cursor cursor = db.getBlahInfoCol().find(query);

        // get blahs cohort-strength
        // notice here also have cohort from previous generations
        while (cursor.hasNext()) {
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            String blahId = blahInfo.getObjectId(DBConst.BlahInfo.ID).toString();
            BasicDBObject strength = (BasicDBObject) blahInfo.get(DBConst.BlahInfo.STRENGTH);

            if (strength == null) {
                // some blah didn't get initial strength
                System.out.println("weird!");
            }
            for (String cohortId : strength.keySet()) {
                // only add cohort-strength for this generation
                if (cohortIdList.contains(cohortId))
                    blahStrengthInCohort.get(cohortId).put(blahId, strength.getDouble(cohortId));
            }
        }
        cursor.close();

        return blahStrengthInCohort;
    }

    private HashMap<String, Double> computeAvgBlahStrengthForAllCohort(HashMap<String, HashMap<String, Double>> blahStrengthInCohort) {
        HashMap<String, Double> userCohortStrength = new HashMap<>();
        // take average
        for (String cohortId : blahStrengthInCohort.keySet()) {
            HashMap<String, Double> blahStrength = blahStrengthInCohort.get(cohortId);
            if (blahStrength.size() == 0){
                userCohortStrength.put(cohortId, 0.0);
            }
                else {
                double avg = 0;
                for (String blahId : blahStrength.keySet()) {
                    avg += blahStrength.get(blahId);
                }
                avg /= blahStrength.size();
                userCohortStrength.put(cohortId, avg);
            }
        }
        return userCohortStrength;
    }

    private void updateUserStrength(String userId, String groupId, HashMap<String, Double> cohortStrength) {
        System.out.print("update database...");
        // update user's cohort strength and strength update time
        BasicDBObject values = new BasicDBObject();
        for (String cohortId : cohortStrength.keySet()) {
            values.put(DBConst.UserGroupInfo.STRENGTH + "." + cohortId, cohortStrength.get(cohortId));
        }
        values.put(DBConst.UserGroupInfo.STRENGTH_UPDATE_TIME, new Date());

        BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.USER_ID, new ObjectId(userId));
        query.put(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
        BasicDBObject setter = new BasicDBObject("$set", values);
        db.getUserGroupInfoCol().update(query, setter);
        System.out.println("done");
    }

    private void computeAndUpdateAllStrength(String groupId, String generationId) throws TaskException, StorageException {
        computeAndUpdateAllBlahStrength(groupId, generationId);
        computeAndUpdateAllUserStrength(groupId, generationId);
        produceInboxTask(groupId, generationId);
    }

    private void computeAndUpdateAllBlahStrength(String groupId, String generationId) {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        System.out.println(servicePrefix + groupPrefix + "[blah] [all] compute blah strength");

        List<String> blahIdList = getAllBlahs(groupId);

        int i = 1;
        for (String blahId : blahIdList) {
            try {
                System.out.print(servicePrefix + groupPrefix + "[blah] [all] " );
                System.out.printf("%9d / %9d\t", i++, blahIdList.size());
                HashMap<String, Double> cohortStrength = computeBlahStrength(blahId, groupId, generationId);
                updateBlahStrength(blahId, cohortStrength);
            }
            catch (TaskException e) {

                System.out.println("skipped");
                System.out.println(e.getMessage());
                if (e.type == TaskExceptionType.SKIP) continue;
                else continue; // TODO how to deal with re-compute case for single blah strength task?
            }
        }

        System.out.println(servicePrefix + groupPrefix + "[blah] [all] strength computation finished");
    }

    private void computeAndUpdateAllUserStrength(String groupId, String generationId) {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        System.out.println(servicePrefix + groupPrefix + "[user] [all] compute user strength");

        List<String> userIdList = getAllUsers(groupId);

        int j = 1;
        for (String userId : userIdList) {
//            try {
            System.out.print(servicePrefix +  groupPrefix + "[user] [all] ");
            System.out.printf("%9d / %9d\t", j++, userIdList.size());
            HashMap<String, Double> cohortStrength = computeUserStrength(userId, groupId, generationId);
            updateUserStrength(userId, groupId, cohortStrength);
//            }
//            catch (TaskException e) {
//                System.out.println("skipped");
//                if (e.type == TaskExceptionType.SKIP) continue;
//                else continue; // TODO how to deal with re-compute case for single blah strength task?
//            }
        }

        System.out.println(servicePrefix + groupPrefix + "[user] [all] strength computation finished");

    }

    private List<String> getAllBlahs(String groupId) {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        System.out.print(servicePrefix + groupPrefix + "[blah] [all] getting all blahs in group...");
        BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.GROUP_ID, new ObjectId(groupId));
        DBCursor cursor = db.getBlahInfoCol().find(query);

        List<String> blahIdList = new ArrayList<>();
        while (cursor.hasNext()) {
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            blahIdList.add(blahInfo.getObjectId(DBConst.BlahInfo.ID).toString());
        }
        cursor.close();
        System.out.println("done");

        return blahIdList;
    }

    private List<String> getAllUsers(String groupId) {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        System.out.print(servicePrefix + groupPrefix + "[user] [all] getting all users in this group...");
        BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
        DBCursor cursor = db.getUserGroupInfoCol().find(query);

        List<String> userIdList = new ArrayList<>();
        while (cursor.hasNext()) {
            BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
            userIdList.add(userGroupInfo.getObjectId(DBConst.UserGroupInfo.USER_ID).toString());
        }
        cursor.close();
        System.out.println("done");

        return userIdList;
    }

    private void produceInboxTask(String groupId, String generationId) throws StorageException {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        System.out.print(servicePrefix + groupPrefix + "produce inbox task : GENERATE_INBOX_NEW_CLUSTER...");

        BasicDBObject task = new BasicDBObject();
        task.put(AzureConst.InboxTask.TYPE, AzureConst.InboxTask.GENERATE_INBOX_NEW_CLUSTER);
        task.put(AzureConst.InboxTask.GROUP_ID, groupId);
        task.put(AzureConst.InboxTask.GENERATION_ID, generationId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        azure.getInboxTaskQueue().addMessage(message);

        System.out.println("done");
    }
}
