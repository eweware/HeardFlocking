package com.eweware.heardflocking;


import Jama.Matrix;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
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

    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;

    private DBCollection groupsCol;
    private DBCollection blahInfoCol;
    private DBCollection cohortInfoCol;
    private DBCollection generationInfoCol;
    private DBCollection userGroupInfoCol;
    private DBCollection userBlahInfoCol;

    int visibilityTimoutSeconds = 60 / 2;
    long noTaskWaitMillis = 1000 * 10;

    // weights for user-blah utility
    double wV = 0;
    double wO = 1.0;
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
                CloudQueueMessage message = strengthTaskQueue.retrieveMessage(visibilityTimoutSeconds, null, new OperationContext());
                if (message != null) {
                    // Process the message within certain time, and then delete the message.
                    BasicDBObject task = (BasicDBObject) JSON.parse(message.getMessageContentAsString());
                    try {
                        if (processTask(task)) {
                            // success, delete message
                            strengthTaskQueue.deleteMessage(message);
                        } else {
                            // fail, reset message to be visible
                            // notice, if processing task throws exception, the task is then deleted
                            // for now, all tasks are processed successfully or throw exception
                            // no task will be re-process by another worker
                            strengthTaskQueue.updateMessage(message, 0);
                        }
                    }
                    catch (Exception e) {
                        // there is something wrong about the task
                        // task is abandoned, deleted from queue
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                        //strengthTaskQueue.deleteMessage(message);
                    }
                }
                else {
                    Thread.sleep(noTaskWaitMillis);
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

        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");
        userBlahInfoCol = infoDB.getCollection("userBlahInfo");

        System.out.println("done");
    }

    private boolean processTask(BasicDBObject task) throws Exception {
        int taskType = (Integer) task.get(AzureConstants.StrengthTask.TYPE);

        if (taskType == AzureConstants.StrengthTask.COMPUTE_BLAH_STRENGTH) {
            String blahId = (String) task.get(AzureConstants.StrengthTask.BLAH_ID);
            String groupId = (String) task.get(AzureConstants.StrengthTask.GROUP_ID);
            HashMap<String, Double> cohortStrength = computeBlahStrength(blahId, groupId);
            updateBlahStrength(blahId, cohortStrength);
            return true;
        }
        else if (taskType == AzureConstants.StrengthTask.COMPUTE_USER_STRENGTH) {
            String userId = (String) task.get(AzureConstants.StrengthTask.USER_ID);
            String groupId = (String) task.get(AzureConstants.StrengthTask.GROUP_ID);
            HashMap<String, Double> cohortStrength = computeUserStrength(userId, groupId);
            updateUserStrength(userId, groupId, cohortStrength);
            return true;
        }
        else {
            throw new Exception("Undefined task type : " + taskType);
        }
    }

    private HashMap<String, Double> computeBlahStrength(String blahId, String groupId) throws Exception {
        // get user-blah util vector for this blah
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.UserBlahInfo.BLAH_ID, blahId);
        Cursor cursor = userBlahInfoCol.find(query);

        // store userId in list, store counterpart user-blah utility in another list
        List<String> userIdList = new ArrayList<String>();
        ArrayList<Double> userUtilList = new ArrayList<Double>();
        while (cursor.hasNext()) {
            BasicDBObject userBlahInfo = (BasicDBObject) cursor.next();
            String userId = (String) userBlahInfo.get(DBConstants.UserBlahInfo.USER_ID);
            double util = computeUtility(userBlahInfo);
            if (util != 0) {
                userIdList.add(userId);
                userUtilList.add(util);
            }
        }
        cursor.close();
        // convert to double[]
        double[] userUtilVec = new double[userUtilList.size()];
        for (int i = 0; i < userUtilList.size(); i++)
            userUtilVec[i] = userUtilList.get(i);

        // get current generation id for this blah's group
        query = new BasicDBObject(DBConstants.Groups.ID, new ObjectId(groupId));
        BasicDBObject group = (BasicDBObject) groupsCol.findOne(query);
        if (group == null) throw new Exception("Group not found for ID : " + groupId);

        String generationId = (String) group.get(DBConstants.Groups.CURRENT_GENERATION);

        // get cohort list for this generation
        HashMap<String, Integer> cohortIdIndexMap = new HashMap<String, Integer>();
        query = new BasicDBObject(DBConstants.GenerationInfo.ID, new ObjectId(generationId));
        BasicDBObject generationInfo = (BasicDBObject) generationInfoCol.findOne(query);
        if (group == null) throw new Exception("Generation not found for ID : " + generationId);

        BasicDBObject cohortInboxInfo = (BasicDBObject) generationInfo.get(DBConstants.GenerationInfo.COHORT_INBOX_INFO);

        int c = 0;
        for (String cohortId : cohortInboxInfo.keySet())
            cohortIdIndexMap.put(cohortId, c++);

        // get user cohort cluter info
        double[][] userCohortMtx = new double[userIdList.size()][cohortIdIndexMap.size()];

        for (int u = 0; u < userIdList.size(); u++) {
            String userId = userIdList.get(u);
            query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, userId);
            query.put(DBConstants.UserGroupInfo.GROUP_ID, groupId);

            BasicDBObject userGroupInfo = (BasicDBObject) userGroupInfoCol.findOne(query);
            if (userGroupInfo == null) {
                System.out.println("User-Group Info not found for userID : " + userId + " groupId : " + groupId);
                //throw new Exception("User-Group Info not found for userID : " + userId + " groupId : " + groupId);
            }
            else {
                List<String> userCohortIdList = (List<String>) userGroupInfo.get(DBConstants.UserGroupInfo.COHORT_LIST);
                for (String cohortId : userCohortIdList) {
                    try {
                        userCohortMtx[u][cohortIdIndexMap.get(cohortId)] = 1;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        // use userCohortMtx and userUtilVec to compute cohortStrengthVec
        Matrix A = new Matrix(userCohortMtx);
        Matrix b = new Matrix(userUtilVec, userUtilVec.length);
        // non-negative least squares, based on JAMA
        Matrix x = NNLSSolver.solveNNLS(A, b);
        double[] cohortStrengthVec = x.getColumnPackedCopy();

        // build cohortIdStrengthMap
        HashMap<String, Double> cohortStrength = new HashMap<String, Double>();
        for (String cohortId : cohortIdIndexMap.keySet()) {
            int index = cohortIdIndexMap.get(cohortId);
            cohortStrength.put(cohortId, cohortStrengthVec[index]);
        }

        return cohortStrength;
    }

    private double computeUtility(BasicDBObject userBlahInfo) {
        // view
        int V = userBlahInfo.getInt(DBConstants.UserBlahInfo.VIEWS, 0);

        // open
        int O = userBlahInfo.getInt(DBConstants.UserBlahInfo.OPENS, 0);

        // comment
        int C = userBlahInfo.getInt(DBConstants.UserBlahInfo.COMMENTS, 0);

        // promote
        // P > 0 upvote:    more people should see this
        // P < 0 downvode:  fewer people should see this
        int P = userBlahInfo.getInt(DBConstants.UserBlahInfo.PROMOTION, 0);

        // compute utility
        return V * wV + O * wO + C * wC + P * wP;
    }

    private void updateBlahStrength(String blahId, HashMap<String, Double> cohortStrength) {
        BasicDBObject values = new BasicDBObject();
        for (String cohortId : cohortStrength.keySet()) {
            values.put(DBConstants.BlahInfo.STRENGTH + "." + cohortId, cohortStrength.get(cohortId));
        }
        values.put(DBConstants.BlahInfo.STRENGTH_UPDATE_TIME, new Date());

        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.BLAH_ID, blahId);
        BasicDBObject setter = new BasicDBObject("$set", values);
        blahInfoCol.update(query, setter);
    }

    private HashMap<String, Double> computeUserStrength(String userId, String groupId) {
        // get blahinfo authored by this user in this group
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.BlahInfo.AUTHOR_ID, userId);
        query.put(DBConstants.BlahInfo.GROUP_ID, groupId);

        // only consider blahs authored by the user in the recent certain number of months
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -12);
        Date earliestRelevantDate = cal.getTime();
        query.put(DBConstants.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate));

        // get blahs cohort-strength
        List<HashMap<String, Double>> blahStrengthList = new ArrayList<HashMap<String, Double>>();
        Cursor cursor = blahInfoCol.find(query);
        while (cursor.hasNext()) {
            HashMap<String, Double> blahCohortStrength = new HashMap<String, Double>();
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            BasicDBObject strength = (BasicDBObject) blahInfo.get(DBConstants.BlahInfo.STRENGTH);
            for (String cohortId : strength.keySet()) {
                blahCohortStrength.put(cohortId, (Double) strength.get(cohortId));
            }
            blahStrengthList.add(blahCohortStrength);
        }
        cursor.close();

        // get user's cohort information
        query = new BasicDBObject();
        query.put(DBConstants.UserGroupInfo.USER_ID, userId);
        query.put(DBConstants.UserGroupInfo.GROUP_ID, groupId);
        BasicDBObject userGroupInfo = (BasicDBObject) userGroupInfoCol.findOne(query);
        List<String> cohortIdList = (List<String>) userGroupInfo.get(DBConstants.UserGroupInfo.COHORT_LIST);
        // initialize user's cohort-strength
        HashMap<String, Double> userCohortStrength = new HashMap<String, Double>();
        for (String cohortId : cohortIdList) {
            userCohortStrength.put(cohortId, 0.0);
        }

        // compute user's cohort-strength
        // take average
        for (HashMap<String, Double> blahCohortStrength : blahStrengthList) {
            for (String cohortId : cohortIdList) {
                double sum = userCohortStrength.get(cohortId);
                sum += blahCohortStrength.get(cohortId);
                userCohortStrength.put(cohortId, sum);
            }
        }
        for (String cohortId : cohortIdList) {
            double avg = userCohortStrength.get(cohortId);
            avg /= blahStrengthList.size();
            userCohortStrength.put(cohortId, avg);
        }
        return userCohortStrength;
    }

    private void updateUserStrength(String userId, String groupId, HashMap<String, Double> cohortStrength) {
        BasicDBObject values = new BasicDBObject();
        for (String cohortId : cohortStrength.keySet()) {
            values.put(DBConstants.UserGroupInfo.STRENGTH + "." + cohortId, cohortStrength.get(cohortId));
        }
        values.put(DBConstants.UserGroupInfo.STRENGTH_UPDATE_TIME, new Date());

        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, userId);
        query.put(DBConstants.UserGroupInfo.GROUP_ID, groupId);
        BasicDBObject setter = new BasicDBObject("$set", values);
        blahInfoCol.update(query, setter);
    }
}
