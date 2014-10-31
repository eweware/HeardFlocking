package com.eweware.heardflocking.strength;


import Jama.Matrix;
import Jama.util.Maths;
import com.eweware.heardflocking.AzureConstants;
import com.eweware.heardflocking.DBConstants;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by weihan on 10/23/14.
 */
public class StrengthTaskWorker {

    private CloudQueueClient queueClient;
    private CloudQueue strengthTaskQueue;
    private CloudQueue inboxTaskQueue;

    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;
    private DB statsDB;

    private DBCollection groupsCol;
    private DBCollection blahInfoCol;
    private DBCollection cohortInfoCol;
    private DBCollection generationInfoCol;
    private DBCollection userGroupInfoCol;
    private DBCollection userBlahStatsCol;

    // each task is given this amount of time for processing before it become visible again in the queue
    final int VISIBLE_TIMEOUT_SECONDS = 60 / 2;
    final long NO_TASK_WAIT_MILLIS = 1000 * 10;

    // author's strength depends on his blahs' strength, only relevant blahs are taken into account
    // a blah is relevant if it was posted within this number of days in the past
    final int RELEVANT_BLAH_PERIOD_DAYS = 365;

    // weights for user-blah utility
    double wV = 1.0;
    double wO = 2.0;
    double wC = 5.0;
    double wP = 10.0;

    public static void main(String[] args) {
        new StrengthTaskWorker().execute();
    }

    private void execute() {
        try {
            initializeQueue();
            initializeMongoDB();
            // continuously get task to work on
            while (true) {
                CloudQueueMessage message = strengthTaskQueue.retrieveMessage(VISIBLE_TIMEOUT_SECONDS, null, new OperationContext());
                if (message != null) {
                    // Process the message within certain time, and then delete the message.
                    BasicDBObject task = (BasicDBObject) JSON.parse(message.getMessageContentAsString());
                    try {
                        processTask(task);
                        strengthTaskQueue.deleteMessage(message);
                    }
                    catch (TaskException e) {
                        // there is something wrong about the task
                        if (e.type == TaskExceptionType.SKIP) {
                            System.out.println(e.getMessage() + ", task skipped");
                            strengthTaskQueue.deleteMessage(message);
                        }
                        else if (e.type == TaskExceptionType.RECOMPUTE) {
                            System.out.println(e.getMessage() + ", task put back to queue");
                            strengthTaskQueue.updateMessage(message, 0);
                        }
                        else
                            e.printStackTrace();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                else {
                    System.out.println("No more tasks, rest for " + NO_TASK_WAIT_MILLIS + " milliseconds.");
                    Thread.sleep(NO_TASK_WAIT_MILLIS);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void initializeQueue() throws Exception {
        System.out.print("Initializing Azure Storage Queue service...");

        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(AzureConstants.STORAGE_CONNECTION_STRING);

        // Create the queue client.
        queueClient = storageAccount.createCloudQueueClient();

        // Retrieve a reference to a queue.
        strengthTaskQueue = queueClient.getQueueReference(AzureConstants.STRENGTH_TASK_QUEUE);
        inboxTaskQueue = queueClient.getQueueReference(AzureConstants.INBOX_TASK_QUEUE);

        // Create the queue if it doesn't already exist.
        strengthTaskQueue.createIfNotExists();
        inboxTaskQueue.createIfNotExists();

        System.out.println("done");
    }

    private void initializeMongoDB() throws UnknownHostException {
        System.out.print("Initializing MongoDB connection... ");

        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");
        statsDB = mongoClient.getDB("statsdb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");
        userBlahStatsCol = statsDB.getCollection("userblahstats");

        System.out.println("done");
    }

    private void processTask(BasicDBObject task) throws TaskException, StorageException {
        int taskType = (Integer) task.get(AzureConstants.StrengthTask.TYPE);

        if (taskType == AzureConstants.StrengthTask.COMPUTE_BLAH_STRENGTH) {
            String blahId = (String) task.get(AzureConstants.StrengthTask.BLAH_ID);
            String groupId = (String) task.get(AzureConstants.StrengthTask.GROUP_ID);
            HashMap<String, Double> cohortStrength = computeBlahStrength(blahId, groupId, false);
            updateBlahStrength(blahId, cohortStrength);
        }
        else if (taskType == AzureConstants.StrengthTask.COMPUTE_USER_STRENGTH) {
            String userId = (String) task.get(AzureConstants.StrengthTask.USER_ID);
            String groupId = (String) task.get(AzureConstants.StrengthTask.GROUP_ID);
            HashMap<String, Double> cohortStrength = computeUserStrength(userId, groupId, false);
            updateUserStrength(userId, groupId, cohortStrength);
        }
        else if (taskType == AzureConstants.StrengthTask.COMPUTE_ALL_STRENGTH) {
            String groupId = (String) task.get(AzureConstants.StrengthTask.GROUP_ID);
            computeAndUpdateAllStrength(groupId);
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

    private HashMap<String, Double> computeBlahStrength(String blahId, String groupId, boolean nextGen) throws TaskException {
        System.out.print("Compute strength blah id : " + blahId + " ...");

        // if compute strength for current generation
        // get current generation id for this blah's group, otherwise get next generation id
        String generationId = getGenerationId(groupId, nextGen);

        // get cohort info for this generation
        BasicDBObject cohortInboxInfo = getCohortInfo(generationId);

        // build cohortId -> index map
        HashMap<String, Integer> cohortIdIndexMap = new HashMap<>();
        int c = 0;
        for (String cohortId : cohortInboxInfo.keySet())
            cohortIdIndexMap.put(cohortId, c++);

        // get user-blah util vector for this blah
        AggregationOutput output = blahAggregation(blahId);

        // declare final cohort-strength result in vector form
        double[] cohortStrengthVec;

        // if the blah doesn't have any activity
        //   if this is for next generation, return cohort strength with all 0.0
        //   else skip this task
        Iterator<DBObject> it = output.results().iterator();
        if (!it.hasNext()) {
            if (!nextGen) throw new TaskException("Error : no user-blah stats for blah : " + blahId, TaskExceptionType.SKIP);
            else {
                cohortStrengthVec = new double[cohortIdIndexMap.size()];
            }
        }
        else {
            // the aggregation is grouped by userId, for each user, compute utility and store
            // add userId and corresponding utility to two corresponding lists
            List<String> userIdList = new ArrayList<>();
            ArrayList<Double> userUtilList = new ArrayList<>();
            for (DBObject userBlahAggregation : output.results()) {
                UserBlahInfo userBlahInfo = new UserBlahInfo(userBlahAggregation, blahId);
                userIdList.add(userBlahInfo.userId.toString());
                userUtilList.add(computeUtility(userBlahInfo));
            }

            // get user cohort cluster info
            // matrix[userIndex][cohortIndex] = 1 when the user is in the cohort, = 0 otherwise
            // there is data inconsistency between userBlahStats and userGroupInfo, delete those rows
            HashSet<Integer> deleteRow = new HashSet<>();
            double[][] userCohortMtx = getUserCohortInfo(userIdList, cohortIdIndexMap, groupId, nextGen, deleteRow);

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

        System.out.print("done");
        return cohortStrength;
    }

    private AggregationOutput blahAggregation(String blahId) {
        // aggregate each user's activities for this blah
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject(DBConstants.UserBlahStats.BLAH_ID, new ObjectId(blahId)));

        BasicDBObject groupFields = new BasicDBObject("_id", "$"+DBConstants.UserBlahStats.USER_ID);
        groupFields.append(DBConstants.UserBlahStats.VIEWS, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.VIEWS));
        groupFields.append(DBConstants.UserBlahStats.OPENS, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.OPENS));
        groupFields.append(DBConstants.UserBlahStats.COMMENTS, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.COMMENTS));
        groupFields.append(DBConstants.UserBlahStats.PROMOTION, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.PROMOTION));
        BasicDBObject groupBy = new BasicDBObject("$group", groupFields);

        List<DBObject> pipeline = Arrays.asList(match, groupBy);
        return userBlahStatsCol.aggregate(pipeline);
    }

    private class UserBlahInfo {
        ObjectId blahId;
        ObjectId userId;

        int views;
        int opens;
        int comments;
        int promotion;

        private UserBlahInfo(DBObject userBlah, String blahId) {
            userId = (ObjectId) userBlah.get("_id");
            this.blahId = new ObjectId(blahId);

            Integer obj;
            obj = (Integer) userBlah.get(DBConstants.UserBlahStats.VIEWS);
            views = obj == null ? 0 : obj;
            obj = (Integer) userBlah.get(DBConstants.UserBlahStats.OPENS);
            opens = obj == null ? 0 : obj;
            obj = (Integer) userBlah.get(DBConstants.UserBlahStats.COMMENTS);
            comments = obj == null ? 0 : obj;
            obj = (Integer) userBlah.get(DBConstants.UserBlahStats.PROMOTION);
            promotion = obj == null ? 0 : obj;
        }
    }

    private String getGenerationId(String groupId, boolean nextGen) throws TaskException {
        BasicDBObject query = new BasicDBObject(DBConstants.Groups.ID, new ObjectId(groupId));
        BasicDBObject group = (BasicDBObject) groupsCol.findOne(query);
        if (group == null) throw new TaskException("Error : group not found for ID : " + groupId, TaskExceptionType.SKIP);
        if (nextGen)
            return group.getString(DBConstants.Groups.NEXT_GENERATION);
        else
            return group.getString(DBConstants.Groups.CURRENT_GENERATION);
    }

    private BasicDBObject getCohortInfo(String generationId) throws TaskException {
        BasicDBObject query = new BasicDBObject(DBConstants.GenerationInfo.ID, new ObjectId(generationId));
        BasicDBObject generationInfo = (BasicDBObject) generationInfoCol.findOne(query);
        if (generationInfo == null) throw new TaskException("Error : generation info not found for ID : " + generationId, TaskExceptionType.SKIP);
        return (BasicDBObject) generationInfo.get(DBConstants.GenerationInfo.COHORT_INFO);
    }

    private double computeUtility(UserBlahInfo userBlahInfo) {
        // P > 0 upvote:    more people should see this
        // P < 0 downvode:  fewer people should see this

        // if a user view a blah but didn't open, that's a sign for negative utility
        double util = userBlahInfo.opens * wO +
                userBlahInfo.comments * wC +
                userBlahInfo.promotion * wP -
                Math.signum(userBlahInfo.views) * wV;
        return util;
    }

    private double[][] getUserCohortInfo(List<String> userIdList, HashMap<String, Integer> cohortIdIndexMap, String groupId, boolean nextGen, HashSet<Integer> deleteRow) throws TaskException {

        double[][] userCohortMtx = new double[userIdList.size()][cohortIdIndexMap.size()];

        for (int u = 0; u < userIdList.size(); u++) {
            String userId = userIdList.get(u);
            BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, new ObjectId(userId));
            query.put(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(groupId));

            BasicDBObject userGroupInfo = (BasicDBObject) userGroupInfoCol.findOne(query);

            if (userGroupInfo == null) {
                // this happens because there are users who act on this group's blah but is not in that group
                // data inconsistent!
                deleteRow.add(u);
//                throw new TaskException("Error : user-group info not found for userID : " + userId + " groupId : " + groupId, TaskExceptionType.SKIP);
            }
            else {
                List<ObjectId> userCohortIdObjList;
                if (nextGen) {
                    userCohortIdObjList = (List<ObjectId>) userGroupInfo.get(DBConstants.UserGroupInfo.NEXT_COHORT_LIST);
                }
                else {
                    userCohortIdObjList = (List<ObjectId>) userGroupInfo.get(DBConstants.UserGroupInfo.CURRENT_COHORT_LIST);
                }
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
        System.out.print(", and update database...");
        BasicDBObject values = new BasicDBObject();
        for (String cohortId : cohortStrength.keySet()) {
            values.put(DBConstants.BlahInfo.STRENGTH + "." + cohortId, cohortStrength.get(cohortId));
        }
        values.put(DBConstants.BlahInfo.STRENGTH_UPDATE_TIME, new Date());

        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.ID, new ObjectId(blahId));
        BasicDBObject setter = new BasicDBObject("$set", values);
        blahInfoCol.update(query, setter);
        System.out.println("done");
    }

    private HashMap<String, Double> computeUserStrength(String userId, String groupId, boolean nextGen) {
        System.out.print("Compute strength user id : " + userId + " ...");

        // get cohort list for this group for current generation or next generation
        List<String> cohortIdList = getCohortIdList(groupId, nextGen);

        // for all recent blahs authored by this user, get the blahs' cohort strength info
        // cohortId -> (blahId -> strength)
        HashMap<String, HashMap<String, Double>> blahStrengthInCohort = getBlahStrengthInCohort(userId, groupId, cohortIdList);

        // compute user's cohort-strength
        HashMap<String, Double> userCohortStrength = computeAvgBlahStrengthForAllCohort(blahStrengthInCohort);

        System.out.print("done");
        return userCohortStrength;
    }

    private List<String> getCohortIdList(String groupId, boolean nextGen) {
        // get group info
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.Groups.ID, new ObjectId(groupId));
        BasicDBObject group = (BasicDBObject) groupsCol.findOne(query);
        // get this group's current generationId or next generationId
        ObjectId generationIdObj;
        if (nextGen)
            generationIdObj = group.getObjectId(DBConstants.Groups.NEXT_GENERATION);
        else
            generationIdObj = group.getObjectId(DBConstants.Groups.CURRENT_GENERATION);

        // get this generation's cohort list
        query = new BasicDBObject(DBConstants.GenerationInfo.ID, generationIdObj);
        BasicDBObject generation = (BasicDBObject) generationInfoCol.findOne(query);
        BasicDBObject cohortInfo = (BasicDBObject) generation.get(DBConstants.GenerationInfo.COHORT_INFO);

        List<String> cohortIdList = new ArrayList<>();
        for (String cohortId : cohortInfo.keySet()) {
            cohortIdList.add(cohortId);
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
        query.put(DBConstants.BlahInfo.AUTHOR_ID, new ObjectId(userId));
        query.put(DBConstants.BlahInfo.GROUP_ID, new ObjectId(groupId));

        // only consider blahs authored by the user in the recent certain number of months
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -RELEVANT_BLAH_PERIOD_DAYS);
        Date earliestRelevantDate = cal.getTime();
        query.put(DBConstants.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate));

        Cursor cursor = blahInfoCol.find(query);

        // get blahs cohort-strength
        // notice here also have cohort from previous generations
        while (cursor.hasNext()) {
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            String blahId = blahInfo.getObjectId(DBConstants.BlahInfo.ID).toString();
            BasicDBObject strength = (BasicDBObject) blahInfo.get(DBConstants.BlahInfo.STRENGTH);

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
            double avg = 0;
            for (String blahId : blahStrength.keySet()) {
                avg += blahStrength.get(blahId);
            }
            avg /= blahStrength.size();
            userCohortStrength.put(cohortId, avg);
        }
        return userCohortStrength;
    }

    private void updateUserStrength(String userId, String groupId, HashMap<String, Double> cohortStrength) {
        System.out.print(" and update database...");
        // update user's cohort strength and strength update time
        BasicDBObject values = new BasicDBObject();
        for (String cohortId : cohortStrength.keySet()) {
            values.put(DBConstants.UserGroupInfo.STRENGTH + "." + cohortId, cohortStrength.get(cohortId));
        }
        values.put(DBConstants.UserGroupInfo.STRENGTH_UPDATE_TIME, new Date());

        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, new ObjectId(userId));
        query.put(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
        BasicDBObject setter = new BasicDBObject("$set", values);
        userGroupInfoCol.update(query, setter);
        System.out.println("done");
    }

    private void computeAndUpdateAllStrength(String groupId) throws TaskException, StorageException {
        // compute all blahs' strength

        System.out.println("Start all blahs' strength for groupd : " + groupId);

        List<String> blahIdList = getAllBlahs(groupId);

        int i = 1;
        for (String blahId : blahIdList) {
            try {
                System.out.print(i++ + "/" + blahIdList.size() + "\t");
                HashMap<String, Double> cohortStrength = computeBlahStrength(blahId, groupId, true);
                updateBlahStrength(blahId, cohortStrength);
            }
            catch (TaskException e) {

                System.out.println("skipped");
                System.out.println(e.getMessage());
                if (e.type == TaskExceptionType.SKIP) continue;
                else continue; // TODO how to deal with re-compute case for single blah strength task?
            }
        }

        System.out.println("All blah's strength computation is done");

        // compute all users' strength
        System.out.println("Start computing all users' strength for groupd : " + groupId);

        List<String> userIdList = getAllUsers(groupId);

        int j = 1;
        for (String userId : userIdList) {
//            try {
                System.out.print(j++ + "/" + userIdList.size() + "\t");
                HashMap<String, Double> cohortStrength = computeUserStrength(userId, groupId, true);
                updateUserStrength(userId, groupId, cohortStrength);
//            }
//            catch (TaskException e) {
//                System.out.println("skipped");
//                if (e.type == TaskExceptionType.SKIP) continue;
//                else continue; // TODO how to deal with re-compute case for single blah strength task?
//            }
        }

        System.out.println("All users' strength computation is done");

        // produce new cluster inbox task
        produceInboxTask(groupId);
    }

    private List<String> getAllBlahs(String groupId) {
        System.out.print("Getting all blahs in this group...");
        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.GROUP_ID, new ObjectId(groupId));
        DBCursor cursor = blahInfoCol.find(query);

        List<String> blahIdList = new ArrayList<>();
        while (cursor.hasNext()) {
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            blahIdList.add(blahInfo.getObjectId(DBConstants.BlahInfo.ID).toString());
        }
        cursor.close();
        System.out.println("done");

        return blahIdList;
    }

    private List<String> getAllUsers(String groupId) {
        System.out.print("Getting all blahs in this group...");
        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
        DBCursor cursor = userGroupInfoCol.find(query);

        List<String> userIdList = new ArrayList<>();
        while (cursor.hasNext()) {
            BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
            userIdList.add(userGroupInfo.getObjectId(DBConstants.UserGroupInfo.USER_ID).toString());
        }
        cursor.close();
        System.out.println("done");

        return userIdList;
    }

    private void produceInboxTask(String groupId) throws StorageException {
        System.out.print("Producing new cluster inbox task...");

        BasicDBObject task = new BasicDBObject();
        task.put(AzureConstants.InboxTask.TYPE, AzureConstants.InboxTask.GENERATE_INBOX_NEW_CLUSTER);
        task.put(AzureConstants.InboxTask.GROUP_ID, groupId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        inboxTaskQueue.addMessage(message);

        System.out.println("done");
    }
}
