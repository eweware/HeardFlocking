package com.eweware.heardflocking.strength;


import Jama.Matrix;
import com.eweware.heardflocking.AzureConstants;
import com.eweware.heardflocking.DBConstants;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.*;
import com.mongodb.util.Hash;
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

        // Create the queue if it doesn't already exist.
        strengthTaskQueue.createIfNotExists();

        System.out.println("done");
    }

    private void initializeMongoDB() throws UnknownHostException {
        System.out.print("Initializing MongoDB connection... ");

        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DEV_DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");
        statsDB = mongoClient.getDB("statsdb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");
        userBlahStatsCol = statsDB.getCollection("userBlahInfo");

        System.out.println("done");
    }

    private void processTask(BasicDBObject task) throws TaskException {
        int taskType = (Integer) task.get(AzureConstants.StrengthTask.TYPE);

        if (taskType == AzureConstants.StrengthTask.COMPUTE_BLAH_STRENGTH) {
            String blahId = (String) task.get(AzureConstants.StrengthTask.BLAH_ID);
            String groupId = (String) task.get(AzureConstants.StrengthTask.GROUP_ID);
            HashMap<String, Double> cohortStrength = computeBlahStrength(blahId, groupId);
            updateBlahStrength(blahId, cohortStrength);
        }
        else if (taskType == AzureConstants.StrengthTask.COMPUTE_USER_STRENGTH) {
            String userId = (String) task.get(AzureConstants.StrengthTask.USER_ID);
            String groupId = (String) task.get(AzureConstants.StrengthTask.GROUP_ID);
            HashMap<String, Double> cohortStrength = computeUserStrength(userId, groupId);
            updateUserStrength(userId, groupId, cohortStrength);
        }
        else if (taskType == AzureConstants.StrengthTask.COMPUTE_ALL_STRENGTH) {

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

    private HashMap<String, Double> computeBlahStrength(String blahId, String groupId) throws TaskException {
        System.out.print("Compute strength blah id : " + blahId + " ...");
        // get user-blah util vector for this blah
        // aggregate each user's activities for this blah
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject(DBConstants.UserBlahStats.BLAH_ID, blahId));

        BasicDBObject groupFields = new BasicDBObject(DBConstants.UserBlahStats.USER_ID, DBConstants.UserBlahStats.USER_ID);
        groupFields.append(DBConstants.UserBlahStats.VIEWS, new BasicDBObject("$sum", DBConstants.UserBlahStats.VIEWS));
        groupFields.append(DBConstants.UserBlahStats.OPENS, new BasicDBObject("$sum", DBConstants.UserBlahStats.OPENS));
        groupFields.append(DBConstants.UserBlahStats.COMMENTS, new BasicDBObject("$sum", DBConstants.UserBlahStats.COMMENTS));
        groupFields.append(DBConstants.UserBlahStats.PROMOTION, new BasicDBObject("$sum", DBConstants.UserBlahStats.PROMOTION));
        BasicDBObject groupBy = new BasicDBObject("$group", groupFields);

        List<DBObject> pipeline = Arrays.asList(match, groupBy);
        AggregationOutput output = userBlahStatsCol.aggregate(pipeline);

        // if the blah doesn't have any activity, return
        Iterator<DBObject> it = output.results().iterator();
        if (!it.hasNext()) throw new TaskException("Error : no user-blah stats for blah : " + blahId, TaskExceptionType.SKIP);

        // the aggregation is grouped by userId, for each user, compute utility and store
        List<String> userIdList = new ArrayList<>();
        ArrayList<Double> userUtilList = new ArrayList<>();
        for (DBObject userBlah : output.results()) {
            UserBlahInfo userBlahInfo = new UserBlahInfo(userBlah);

            userIdList.add(userBlahInfo.userId);

            userUtilList.add(computeUtility(userBlahInfo));
        }

        // convert to double[]
        double[] userUtilVec = new double[userUtilList.size()];
        for (int i = 0; i < userUtilList.size(); i++)
            userUtilVec[i] = userUtilList.get(i);

        // get current generation id for this blah's group
        BasicDBObject query = new BasicDBObject(DBConstants.Groups.ID, new ObjectId(groupId));
        BasicDBObject group = (BasicDBObject) groupsCol.findOne(query);
        if (group == null) throw new TaskException("Error : group not found for ID : " + groupId, TaskExceptionType.SKIP);

        String generationId = group.getString(DBConstants.Groups.CURRENT_GENERATION);

        // get cohort list for this generation
        HashMap<String, Integer> cohortIdIndexMap = new HashMap<>();
        query = new BasicDBObject(DBConstants.GenerationInfo.ID, new ObjectId(generationId));
        BasicDBObject generationInfo = (BasicDBObject) generationInfoCol.findOne(query);
        if (group == null) throw new TaskException("Error : generation not found for ID : " + generationId, TaskExceptionType.SKIP);

        BasicDBObject cohortInboxInfo = (BasicDBObject) generationInfo.get(DBConstants.GenerationInfo.COHORT_INFO);

        // build cohortId -> index map
        int c = 0;
        for (String cohortId : cohortInboxInfo.keySet())
            cohortIdIndexMap.put(cohortId, c++);

        // get user cohort cluter info
        // matrix[userIndex][cohortIndex] = 1 when the user is in the cohort, = 0 otherwise
        double[][] userCohortMtx = new double[userIdList.size()][cohortIdIndexMap.size()];

        for (int u = 0; u < userIdList.size(); u++) {
            String userId = userIdList.get(u);
            query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, userId);
            query.put(DBConstants.UserGroupInfo.GROUP_ID, groupId);

            BasicDBObject userGroupInfo = (BasicDBObject) userGroupInfoCol.findOne(query);

            if (userGroupInfo == null) {
                throw new TaskException("Error : user-group info not found for userID : " + userId + " groupId : " + groupId, TaskExceptionType.SKIP);
            }
            else {
                List<String> userCohortIdList = (List<String>) userGroupInfo.get(DBConstants.UserGroupInfo.COHORT_LIST);
                for (String cohortId : userCohortIdList) {
                    userCohortMtx[u][cohortIdIndexMap.get(cohortId)] = 1;
                }
            }
        }

        // use userCohortMtx and userUtilVec to compute cohortStrengthVec
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
        double[] cohortStrengthVec = x.getColumnPackedCopy();

        // build cohortIdStrengthMap
        HashMap<String, Double> cohortStrength = new HashMap<>();
        for (String cohortId : cohortIdIndexMap.keySet()) {
            int index = cohortIdIndexMap.get(cohortId);
            cohortStrength.put(cohortId, cohortStrengthVec[index]);
        }

        System.out.print("done");
        return cohortStrength;
    }


    private class UserBlahInfo {
        String blahId;
        String userId;

        int views;
        int opens;
        int comments;
        int promotion;

        private UserBlahInfo(DBObject userBlah) {
            userId = (String) userBlah.get(DBConstants.UserBlahStats.USER_ID);
            blahId = (String) userBlah.get(DBConstants.UserBlahStats.BLAH_ID);

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

    private void updateBlahStrength(String blahId, HashMap<String, Double> cohortStrength) {
        System.out.print(" and update database...");
        BasicDBObject values = new BasicDBObject();
        for (String cohortId : cohortStrength.keySet()) {
            values.put(DBConstants.BlahInfo.STRENGTH + "." + cohortId, cohortStrength.get(cohortId));
        }
        values.put(DBConstants.BlahInfo.STRENGTH_UPDATE_TIME, new Date());

        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.ID, blahId);
        BasicDBObject setter = new BasicDBObject("$set", values);
        blahInfoCol.update(query, setter);
        System.out.println("done");
    }

    private HashMap<String, Double> computeUserStrength(String userId, String groupId) {
        System.out.print("Compute strength user id : " + userId + " in group id : " + groupId + " ...");

        // get cohort list for this group
        List<String> cohortIdList = getCohortIdList(groupId);

        // for all recent blahs authored by this user, get the blahs' cohort strength info
        // cohortId -> (blahId -> strength)
        HashMap<String, HashMap<String, Double>> blahStrengthInCohort = getBlahStrengthInCohort(userId, groupId, cohortIdList);

        // compute user's cohort-strength
        HashMap<String, Double> userCohortStrength = computeCohortStrength(blahStrengthInCohort);

        System.out.print("done");
        return userCohortStrength;
    }

    private List<String> getCohortIdList(String groupId) {
        // get this group's current generationId
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.UserGroupInfo.GROUP_ID, groupId);
        BasicDBObject group = (BasicDBObject) groupsCol.findOne(query);
        ObjectId generationId = group.getObjectId(DBConstants.Groups.CURRENT_GENERATION);

        // get this generation's cohort list
        query = new BasicDBObject(DBConstants.GenerationInfo.ID, generationId);
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
        for (String cohortId : cohortIdList)
            blahStrengthInCohort.put(cohortId, new HashMap<>());

        // get blahInfo authored by this user in this group
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.BlahInfo.AUTHOR_ID, userId);
        query.put(DBConstants.BlahInfo.GROUP_ID, groupId);

        // only consider blahs authored by the user in the recent certain number of months
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -RELEVANT_BLAH_PERIOD_DAYS);
        Date earliestRelevantDate = cal.getTime();
        query.put(DBConstants.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate));

        Cursor cursor = blahInfoCol.find(query);

        // get blahs cohort-strength
        while (cursor.hasNext()) {
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            String blahId = blahInfo.getObjectId(DBConstants.BlahInfo.ID).toString();
            BasicDBObject strength = (BasicDBObject) blahInfo.get(DBConstants.BlahInfo.STRENGTH);

            for (String cohortId : strength.keySet()) {
                blahStrengthInCohort.get(cohortId).put(blahId, strength.getDouble(cohortId));
            }
        }
        cursor.close();

        return blahStrengthInCohort;
    }

    private HashMap<String, Double> computeCohortStrength(HashMap<String, HashMap<String, Double>> blahStrengthInCohort) {
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

        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, userId);
        query.put(DBConstants.UserGroupInfo.GROUP_ID, groupId);
        BasicDBObject setter = new BasicDBObject("$set", values);
        blahInfoCol.update(query, setter);
        System.out.println("done");
    }
}
