package com.eweware.heardflocking.cohort;

import com.eweware.heardflocking.*;
import com.eweware.heardflocking.base.AzureConst;
import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.base.HeardAzure;
import com.eweware.heardflocking.base.HeardDB;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by weihan on 10/10/14.
 */
public class CohortWorker{
    public CohortWorker(HeardDB db, HeardAzure azure) {
        this.db = db;
        this.azure = azure;
        getGroups();
    }

    private HeardDB db;
    private HeardAzure azure;

    private final String CLUSTERING_METHOD = ServiceProperties.CohortWorker.CLUSTERING_METHOD;

    // assumed number of cohort for K-means clustering and random clustering
    private int numCohorts = ServiceProperties.CohortWorker.NUM_COHORTS;

    // weights for user-blah utility
    final double wV = ServiceProperties.CohortWorker.WEIGHT_VIEW;
    final double wO = ServiceProperties.CohortWorker.WEIGHT_OPEN;
    final double wC = ServiceProperties.CohortWorker.WEIGHT_COMMENT;
    final double wP = ServiceProperties.CohortWorker.WEIGHT_UPVOTES;
    final double wN = ServiceProperties.CohortWorker.WEIGHT_DOWNVOTES;
    final double wCP = ServiceProperties.CohortWorker.WEIGHT_COMMENT_UPVOTES;
    final double wCN = ServiceProperties.CohortWorker.WEIGHT_COMMENT_DOWNVOTES;

    private final boolean CLUSTERING_RESEARCH = false;
    private final boolean ADD_MATURE_COHORT = false;
    private final boolean OUTPUT_COHORT_INFO_FOR_TEST = false;

    HashMap<String, String> groupNames;

    String groupId;

    // blahIdIndexMap : blahId -> vectorIndex
    HashMap<String, Integer> blahIdIndexMap;
    HashMap<String, Integer> activeBlahIdIndexMap;
    HashSet<String> activeBlahSet;

    final int RECENT_BLAH_DAYS = ServiceProperties.CohortWorker.RECENT_BLAH_DAYS;
    final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

    // userIdIndexMapInGroup : groupId -> (userId -> vectorIndex)
    HashMap<String, Integer> userIdIndexMap;
    HashSet<String> activeUserSet;

    // blahUtilMapPerUser : userId -> (blahId -> utility)
    HashMap<String, HashMap<String, Double>> blahUtilMapPerUser;

    // cohort clustering result
    ArrayList<Integer>[] cluster; // userIndex -> List of cohortIndex
    String[] cohortIndexIdMap; // cohortIndex -> cohortId
    HashMap<String, List<String>> userPerCohort; // cohortId -> List of userId
    HashMap<String, List<String>> cohortPerUser; // userId -> List of chortId

    private int QUEUE_VISIBLE_TIMEOUT_SECONDS = ServiceProperties.CohortWorker.QUEUE_VISIBLE_TIMEOUT_SECONDS;
    private long NO_TASK_WAIT_MILLIS = ServiceProperties.CohortWorker.NO_TASK_WAIT_MILLIS;

    private String servicePrefix = "[CohortWorker] ";
    private String groupPrefix;

    public void execute() {
        try {
            // continuously get task to work on
            while (true) {
                CloudQueueMessage message = azure.getCohortTaskQueue().retrieveMessage(QUEUE_VISIBLE_TIMEOUT_SECONDS, null, new OperationContext());
                if (message != null) {
                    // Process the message within certain time, and then delete the message.
                    BasicDBObject task = (BasicDBObject) JSON.parse(message.getMessageContentAsString());
                    try {
                        processTask(task);
                        azure.getCohortTaskQueue().deleteMessage(message);
                    } catch (TaskException e) {
                        // there is something wrong about the task
                        if (e.type == TaskExceptionType.SKIP) {
                            System.out.println(e.getMessage() + ", task skipped");
                            azure.getCohortTaskQueue().deleteMessage(message);
                        } else if (e.type == TaskExceptionType.RECOMPUTE) {
                            System.out.println(e.getMessage() + ", task put back to queue");
                            azure.getCohortTaskQueue().updateMessage(message, 0);
                        } else
                            e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println(servicePrefix + "no task, sleep for " + NO_TASK_WAIT_MILLIS + " milliseconds");
                    Thread.sleep(NO_TASK_WAIT_MILLIS);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processTask(BasicDBObject task) throws Exception {
        int taskType = (Integer) task.get(AzureConst.CohortTask.TYPE);

        if (taskType == AzureConst.CohortTask.RECLUSTER) {
            groupId = (String) task.get(AzureConst.CohortTask.GROUP_ID);
            recluster();
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

    private void recluster() throws Exception {
        groupPrefix = "[" + groupNames.get(groupId) + "] ";

        System.out.println();
        System.out.println(servicePrefix + groupPrefix + " start clustering");

        // count number of blahs in this group
        // assign a vector index for each blah
        countAndIndexBlah();
        System.out.print(servicePrefix + groupPrefix + " #blah : " + blahIdIndexMap.size());

        // count number of users in this group
        // assign a index for each user
        countAndIndexUser();
        System.out.print("\t#users : " + userIdIndexMap.size());

        // compute user-blah utility
        computeUtilityAll();
        System.out.print("\t#blah active : " + activeBlahSet.size());
        System.out.print("\t#user active : " + activeUserSet.size());
        System.out.println();

        // build id - index map for only active blah, we don't need inactive blah for clustering
        indexActiveBlah();

        // convert blahUtilMapPerUser into matrix, using blahIdIndexMap and userIdIndexMap
        double[][] data = getInterestMatrix();

        // to do separate research on the data, output to files
        if (CLUSTERING_RESEARCH) {
            System.out.println("Writing data to local files for research, no clustering is done.");
            outputResearchFiles(data);
            return;
        }

        if (CLUSTERING_METHOD.equals("trivial")) {
            numCohorts = 1;
            trivialClustering();
        } else if (CLUSTERING_METHOD.equals("kmeans")) {
            kmeansClustering(data);
        } else if (CLUSTERING_METHOD.equals("random")) {
            randomClustering();
        } else {
            numCohorts = 1;
            trivialClustering();
        }
        System.out.println(servicePrefix + groupPrefix + " " + numCohorts + " cohorts generated");

        // put result into cohort hashmap
        generateCohortHashMaps();

        // write cohort info into mongo
        updateMongoAndQueue();

        System.out.println();

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

    private void countAndIndexBlah() {
        BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.GROUP_ID, new ObjectId(groupId));
        // only use blah created within N days
        Date relevantDate = new Date(new Date().getTime() - RECENT_BLAH_DAYS * MILLIS_PER_DAY);
        query.append(DBConst.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", relevantDate));

        DBCursor cursor = db.getBlahInfoCol().find(query);

        int blahCount = 0;
        blahIdIndexMap = new HashMap<>();

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            String blahId = obj.getObjectId(DBConst.BlahInfo.ID).toString();
            // count blah and assign index for each blah
            blahIdIndexMap.put(blahId, blahCount);
            blahCount++;
        }
        cursor.close();
    }

    private void countAndIndexUser() {
        BasicDBObject queryGroup = new BasicDBObject(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
        DBCursor cursor = db.getUserGroupInfoCol().find(queryGroup);

        int userCount = 0;
        userIdIndexMap = new HashMap<>();

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            String userId = obj.get(DBConst.UserGroupInfo.USER_ID).toString();
            // count user and assign index for each user
            userIdIndexMap.put(userId, userCount);
            userCount++;
        }
        cursor.close();
    }

    private void computeUtilityAll() {
        activeUserSet = new HashSet<>();
        activeBlahSet = new HashSet<>();

        blahUtilMapPerUser = new HashMap<>();

        // for all blahs, aggregate all activities about it in userBlahStats
        // we could have loop through all users in the group, and aggregate group by blahs,
        // but in that case we can not restrict blahs to only recently created ones
        for (String blahId : blahIdIndexMap.keySet()) {
            // aggregate every user's activity to this blah
            Cursor cursor = blahAggregation(blahId);

            // if the blah doesn't have any activity, skip
            if (cursor.hasNext()) {
                activeBlahSet.add(blahId);
            }
            else continue;

            // the aggregation is grouped by userId, for each user, compute utility and store
            while (cursor.hasNext()) {
                BasicDBObject userBlah = (BasicDBObject) cursor.next();
                UserBlahInfo userBlahInfo = new UserBlahInfo(userBlah, blahId);

                // if this is a new user, count it
                if (!activeUserSet.contains(userBlahInfo.userId)) {
                    activeUserSet.add(userBlahInfo.userId.toString());
                }

                double util = computeUtility(userBlahInfo);

                // utilityBlah : blahId -> utility
                HashMap<String, Double> utilityBlah = blahUtilMapPerUser.get(userBlahInfo.userId);
                // if this is the first blah for this user, create the blahId -> utility hashmap for him
                if (utilityBlah == null) {
                    utilityBlah = new HashMap<>();
                    utilityBlah.put(blahId, util);
                    blahUtilMapPerUser.put(userBlahInfo.userId.toString(), utilityBlah);
                } else {
                    utilityBlah.put(blahId, util);
                }
            }
        }
    }

    private Cursor blahAggregation(String blahId) {
        // match blahId, group by userId, sum activities
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject(DBConst.UserBlahStats.BLAH_ID, new ObjectId(blahId)));

        BasicDBObject groupFields = new BasicDBObject("_id", "$"+ DBConst.UserBlahStats.USER_ID);
        groupFields.append(DBConst.UserBlahStats.VIEWS, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.VIEWS));
        groupFields.append(DBConst.UserBlahStats.OPENS, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.OPENS));
        groupFields.append(DBConst.UserBlahStats.COMMENTS, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.COMMENTS));
        groupFields.append(DBConst.UserBlahStats.COMMENT_UPVOTES, new BasicDBObject("$sum", "$"+ DBConst.UserBlahStats.COMMENT_UPVOTES));
        BasicDBObject group = new BasicDBObject("$group", groupFields);

        List<DBObject> pipeline = Arrays.asList(match, group);

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

    private void indexActiveBlah() throws Exception {
        activeBlahIdIndexMap = new HashMap<>();
        int idx = 0;
        for (String blahId : activeBlahSet) {
            activeBlahIdIndexMap.put(blahId, idx);
            idx++;
        }
        // check numbers are correct
        if (activeBlahIdIndexMap.size() != activeBlahSet.size()) throw new Exception(" Error: active blah number inconsistent!");
    }

    private double[][] getInterestMatrix() {
        // default set to 0
        // only use active blah to do clustering
        double[][] data = new double[userIdIndexMap.size()][activeBlahSet.size()];

        // for each user
        for (String userId : blahUtilMapPerUser.keySet()) {
            // put user-blah utility into vectors
            // utilityBlah : (blahId -> utility)
            HashMap<String, Double> utilityBlah = blahUtilMapPerUser.get(userId);
            // for each blah that the user interacted with
            for (String blahId : utilityBlah.keySet()) {
                // check data consistency:
                // there may be user in statsdb.userblahstats but not in infodb.userGroupInfo
                // ignore these users for now
                if (userIdIndexMap.get(userId) != null)
                    data[userIdIndexMap.get(userId)][activeBlahIdIndexMap.get(blahId)] = utilityBlah.get(blahId);
            }
        }
        return data;
    }

    private void trivialClustering() {
        System.out.print(servicePrefix + groupPrefix + " trivial clustering...");

        numCohorts = 1;
        cluster = new ArrayList[userIdIndexMap.size()];

        // assign all users to the same cohort
        for (int u = 0; u < userIdIndexMap.size(); u++) {
            cluster[u] = new ArrayList<>();
            cluster[u].add(0);
        }
        System.out.println("done");
    }

    private void randomClustering() {
        System.out.print(servicePrefix + groupPrefix + " random clustering...");
        cluster = new ArrayList[userIdIndexMap.size()];
        Random rand = new Random();
        for (int u = 0; u < userIdIndexMap.size(); u++) {
            cluster[u] = new ArrayList<>();
            for (int i = 0; i < numCohorts; i++) {
                if (rand.nextBoolean()) {
                    cluster[u].add(i);
                }
            }
            // make sure the user is in at least one cluster
            if (cluster[u].size() == 0) {
                cluster[u].add(0);
            }
        }
        System.out.println("done");
    }

    private void kmeansClustering(double[][] data) throws Exception {
        System.out.print(servicePrefix + groupPrefix + " k-means clustering...");
        // k-means to cluster users
        int[] kmeansResult;
        kmeansResult = KMeansClustering.run(data, numCohorts);
        if (kmeansResult == null) {
            throw new Exception("Error : k-means return null");
        }
        else {
            // turn k-means result into cluster form
            cluster = new ArrayList[kmeansResult.length];
            for (int u = 0; u < kmeansResult.length; u++) {
                cluster[u] = new ArrayList<>();
                cluster[u].add(kmeansResult[u]);
            }
        }
        System.out.println("done");
    }

    private void outputResearchFiles(double[][] data) {
        //long version = new Date().getTime() / 1000;
        String version = "";
        String dataFileName = "research/" + version + "data_" + groupNames.get(groupId).toLowerCase().replace(' ', '_') + ".csv";

        try {
            FileWriter writer = new FileWriter(dataFileName);

            for (int n = 0; n < data.length; n++) {
                for (int m = 0; m < data[0].length; m++) {
                    writer.append(new Integer((int)data[n][m]).toString());
                    if (m != data[0].length - 1) {
                        writer.append(",");
                    }
                }
                writer.append("\n");
                writer.flush();
            }
            writer.close();

        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    // cluster input : userIndex -> list of clusterIndex
    private void generateCohortHashMaps() {
        // generate cohortIndex->cohortId map
        cohortIndexIdMap = new String[numCohorts];
        for (int c = 0; c < numCohorts; c++) {
            cohortIndexIdMap[c] = new ObjectId().toString();
        }

        // userId -> list of cohortId
        cohortPerUser = new HashMap<>();
        for (String userId : userIdIndexMap.keySet()) {
            int userIdx = userIdIndexMap.get(userId);
            ArrayList<String> cohortIds = new ArrayList<>();
            for (int cohortIdx : cluster[userIdx]) {
                cohortIds.add(cohortIndexIdMap[cohortIdx]);
            }
            cohortPerUser.put(userId, cohortIds);
        }

        // cohortId -> list of userId
        userPerCohort = new HashMap<>();
        for (int c = 0; c < numCohorts; c++) {
            userPerCohort.put(cohortIndexIdMap[c], new ArrayList<>());
        }
        // add userId
        for (String userId : userIdIndexMap.keySet()) {
            int userIdx = userIdIndexMap.get(userId);
            for (int cohortIdx : cluster[userIdx]) {
                String cohortId = cohortIndexIdMap[cohortIdx];
                userPerCohort.get(cohortId).add(userId);
            }
        }
    }

    private void updateMongoAndQueue() throws Exception {

        // make a default cohort
        ObjectId defaultCohortIdObj = new ObjectId();

        // insert cohort information into infodb.cohortInfo
        insertAllCohortInfo(defaultCohortIdObj);

        // insert cohort generation info into infodb.generationInfo
        String generationId = insertGenerationInfo(defaultCohortIdObj);

        // write user's cohort info into infodb.usreGroupInfo
        updateUserGroupInfo(generationId);

        // produce "compute all blah and user strength in this group" task and enqueue
        produceStrengthTask(generationId);
    }

    private void insertAllCohortInfo(ObjectId defaultCohortIdObj) {
        System.out.print(servicePrefix + groupPrefix +" write new cohort information to database...");

        for (String cohortId : userPerCohort.keySet()) {
            List<String> userIdList = userPerCohort.get(cohortId);
            insertCohortInfo(new ObjectId(cohortId), userIdList.size());
        }

        // insert default cohort
        insertCohortInfo(defaultCohortIdObj, -1L);

        System.out.println("done");
    }

    private void insertCohortInfo(ObjectId cohortIdObj, long userNum) {
        BasicDBObject cohortInfo = new BasicDBObject();
        cohortInfo.put(DBConst.CohortInfo.ID, cohortIdObj);
        cohortInfo.put(DBConst.CohortInfo.NUM_USERS, userNum);
//        cohortInfo.put(DBConst.CohortInfo.FIRST_INBOX, -1L);
//        cohortInfo.put(DBConst.CohortInfo.LAST_INBOX, -1L);
        // we could add user list for each cohort, but it this necessary?
//            cohortInfo.put(DBConstants.CohortInfo.USER_LIST, convertIdList(userIdList));
        db.getCohortInfoCol().insert(cohortInfo);
    }

    private String insertGenerationInfo(ObjectId defaultCohortIdObj) {
        System.out.print(servicePrefix + groupPrefix + " write new generation information to database...");
        // make cohort list, exclude the default cohort from this list
        List<ObjectId> cohortList = new ArrayList<>();
//        cohortList.add(defaultCohortIdObj);
        for (String cohortId : userPerCohort.keySet()) {
            cohortList.add(new ObjectId(cohortId));
        }
        // make generation info document
        ObjectId generationIdObj = new ObjectId();
        BasicDBObject generationInfo = new BasicDBObject(DBConst.GenerationInfo.ID, generationIdObj);
        generationInfo.put(DBConst.GenerationInfo.CREATE_TIME, new Date());
        generationInfo.put(DBConst.GenerationInfo.GROUP_ID, new ObjectId(groupId));
        generationInfo.put(DBConst.GenerationInfo.COHORT_LIST, cohortList);
        generationInfo.put(DBConst.GenerationInfo.DEFAULT_COHORT, defaultCohortIdObj);

        db.getGenerationInfoCol().insert(generationInfo);

        System.out.println("done");

//        System.out.println("cohort generation id : " + generationIdObj.toString());
//        for (String cohortId : cohortIndexIdMap) {
//            System.out.println("\tcohort id : " + cohortId);
//        }

        return generationIdObj.toString();
    }

    private void updateUserGroupInfo(String generationId) {
        System.out.print(servicePrefix + groupPrefix + " write cohort information to userGroupInfo ...");

        for (String userId : cohortPerUser.keySet()) {
            List<String> cohortIdList = cohortPerUser.get(userId);

            BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.USER_ID, new ObjectId(userId)).append(DBConst.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
            BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConst.UserGroupInfo.COHORT_GENERATIONS + "." + generationId, convertIdList(cohortIdList)));
            db.getUserGroupInfoCol().update(query, setter);
        }
        System.out.println("done");
    }

    private List<ObjectId> convertIdList(List<String> list) {
        List<ObjectId> objlist = new ArrayList<>();
        for (String strId : list) {
            objlist.add(new ObjectId(strId));
        }
        return objlist;
    }

    private void produceStrengthTask(String generationId) throws Exception {
        System.out.print(servicePrefix + groupPrefix + " produce strength task : COMPUTE_ALL_STRENGTH...");

        BasicDBObject task = new BasicDBObject();
        task.put(AzureConst.StrengthTask.TYPE, AzureConst.StrengthTask.COMPUTE_ALL_STRENGTH);
        task.put(AzureConst.StrengthTask.GROUP_ID, groupId);
        task.put(AzureConst.StrengthTask.GENERATION_ID, generationId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        azure.getStrengthTaskQueue().addMessage(message);

        System.out.println("done");
    }
}
