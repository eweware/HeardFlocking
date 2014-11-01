package com.eweware.heardflocking.cohort;

import com.eweware.heardflocking.AzureConstants;
import com.eweware.heardflocking.DBConstants;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 10/10/14.
 */
public class CohortClusteringService extends TimerTask{

    public CohortClusteringService(String server) {
        DB_SERVER = server;
    }

    public static void main(String[] args) {
        // MongoDB server configuration
        String server = DBConstants.DEV_DB_SERVER;
        if (args.length > 0) {
            if (args[0].equals("dev"))
                server = DBConstants.DEV_DB_SERVER;
            else if (args[0].equals("qa"))
                server = DBConstants.QA_DB_SERVER;
            else if (args[0].equals("prod"))
                server = DBConstants.PROD_DB_SERVER;
            else
            {}
        }

        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set time to run
//        cal.set(Calendar.HOUR_OF_DAY, 20);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // set period
        final int periodHours = 24;

        System.out.println("CohortClustering set to run once for every " + periodHours + " hours, starting at "  + cal.getTime().toString());

        timer.schedule(new CohortClusteringService(server), cal.getTime(), TimeUnit.HOURS.toMillis(periodHours));
    }

    private final boolean CLUSTERING_RESEARCH = false;
    private final boolean TRIVIAL_CLUSTERING = false;
    private final boolean RANDOM_CLUSTERING = true;
    private final boolean ADD_MATURE_COHORT = false;
    private final boolean OUTPUT_COHORT_INFO_FOR_TEST = false;

    // mongo server and databases
    String DB_SERVER;

    MongoClient mongoClient;
    DB userDB;
    DB blahDB;
    DB infoDB;
    DB statsDB;
    DBCollection groupsCol;
    DBCollection blahInfoCol;
    DBCollection userGroupInfoCol;
    DBCollection cohortInfoCol;
    DBCollection generationInfoCol;
    DBCollection userBlahStatsCol;

    HashMap<String, String> groupNames;
    String groupId;
    String groupName;

    // weights for user-blah utility
    final double wV = 1.0;
    final double wO = 2.0;
    final double wC = 5.0;
    final double wP = 10.0;

    // assumed number of cohort for K-means clustering
    int numCohort = 4;

    // blahIdIndexMap : blahId -> vectorIndex
    HashMap<String, Integer> blahIdIndexMap;
    int blahCount; // #blah in the group
    final int RELEVANT_PERIOD_DAYS = 365;
    final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    HashMap<String, Integer> activeBlahIdIndexMap;
    HashSet<String> blahActiveSet;


    // userIdIndexMapInGroup : groupId -> (userId -> vectorIndex)
    HashMap<String, Integer> userIdIndexMap;
    int userCount; // #user in the group
    HashSet<String> userActiveSet;

    // utilityBlahInUser : userId -> (blahId -> utility)
    HashMap<String, HashMap<String, Double>> utilityBlahInUser;
    int blahActiveCount; // #blah with any activity in the group
    int userActiveCount; // #user with any activity in the group

    // mapping cohorts between two generations, new cohortId -> old cohortId
    HashMap<String, String> cohortMapping;
    final double sameCohortThreshold = 0.9;

    // user-blah utility matrix
    double[][] data;

    // cohort clustering result
    ArrayList<Integer>[] cluster; // userIndex -> List of cohortIndex
    String[] cohortIndexIdMap; // cohortIndex -> cohortId
    HashMap<String, List<String>> userPerCohort; // cohortId -> List of userId
    HashMap<String, List<String>> cohortPerUser; // userId -> List of chortId

    private CloudQueueClient queueClient;
    private CloudQueue strengthTaskQueue;

    private final boolean TEST_ONLY_TECH = false;

    @Override
    public void run() {
        try {
            System.out.println();
            System.out.println("########## Start scheduled clustering task ##########  " + new Date());
            System.out.println();

            initializeMongoDB();

            for (String gid : groupNames.keySet()) {
                groupId = gid;
                if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

                System.out.print("Check need for clustering group '" + groupNames.get(gid) + "' id : " + gid + " ...");
                if (!checkNeedClustering(gid)) {
                    System.out.println("NO");
                    continue;
                }
                else {
                    System.out.println("YES");
                }

                groupName = groupNames.get(groupId);

                System.out.println("Start cohort clustering for '" + groupName + "' id : " + groupId);

                // count number of blahs in this group
                // assign a vector index for each blah
                countAndIndexBlah();
                System.out.println("#blah : " + blahCount);

                // count number of users in this group
                // assign a index for each user
                countAndIndexUser();
                System.out.println("#users : " + userCount);

                // compute user-blah utility
                computeUtilityAll();
                System.out.println("#blah active : " + blahActiveCount);
                System.out.println("#user active : " + userActiveCount);

                // build id - index map for only active blah, we don't need inactive blah for clustering
                indexActiveBlah();

                // convert utilityBlahInUser into matrix, using blahIdIndexMap and userIdIndexMap
                getInterestMatrix();

                // to do separate research on the data, output to files
                if (CLUSTERING_RESEARCH) {
                    System.out.println("Writing data to local files for research, no clustering is done.");
                    outputResearchFiles();
                    return;
                }

                if (TRIVIAL_CLUSTERING) {
                    trivialClustering();
                }
                else if (RANDOM_CLUSTERING) {
                    randomClustering();
                }
                else {
                    kmeansClustering();
                }

                // put result into cohort hashmap
                generateCohortHashMaps();

                // find counterpart of new cohorts in previous generation
                //buildCohortMapping();

                // write cohort info into mongo
                outputCohortToMongo();

                //writeFakeBlahCohortStrength();

                // produce "compute all blah and user strength in this group" task and enqueue
                produceStrengthTask();

                System.out.println();
                System.out.println("All done, wait for next round clustering");
            }
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void initializeMongoDB() throws UnknownHostException {
        System.out.print("Initializing MongoDB connection...");

        mongoClient = new MongoClient(DB_SERVER, DBConstants.DB_SERVER_PORT);

        userDB = mongoClient.getDB("userdb");
        blahDB = mongoClient.getDB("blahdb");
        infoDB = mongoClient.getDB("infodb");
        statsDB = mongoClient.getDB("statsdb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");
        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");

        userBlahStatsCol = statsDB.getCollection("userblahstats");

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

    private boolean checkNeedClustering(String id) {
        //TODO add condition to re-cluster cohorts for this group
        return true;
    }

    private void countAndIndexBlah() {
        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.GROUP_ID, new ObjectId(groupId));
        // only use blah created within N days
        Date relevantDate = new Date(new Date().getTime() - RELEVANT_PERIOD_DAYS * MILLIS_PER_DAY);
        query.append(DBConstants.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", relevantDate));

        DBCursor cursor = blahInfoCol.find(query);

        blahCount = 0;
        blahIdIndexMap = new HashMap<>();

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            String blahId = obj.getObjectId(DBConstants.BlahInfo.ID).toString();
            // count blah and assign index for each blah
            blahIdIndexMap.put(blahId, blahCount);
            blahCount++;
        }
        cursor.close();
    }

    private void countAndIndexUser() {
        BasicDBObject queryGroup = new BasicDBObject(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
        DBCursor cursor = userGroupInfoCol.find(queryGroup);

        userCount = 0;
        userIdIndexMap = new HashMap<>();

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            String userId = obj.get(DBConstants.UserGroupInfo.USER_ID).toString();
            // count user and assign index for each user
            userIdIndexMap.put(userId, userCount);
            userCount++;
        }
        cursor.close();
    }

    private void computeUtilityAll() {
        userActiveCount = 0;
        blahActiveCount = 0;
        userActiveSet = new HashSet<>();
        blahActiveSet = new HashSet<>();

        utilityBlahInUser = new HashMap<>();

        // for all blahs, aggregate all activities about it in userBlahStats
        // we could have loop through all users in the group, and aggregate group by blahs,
        // but in that case we can not restrict blahs to only recently created ones
        for (String blahId : blahIdIndexMap.keySet()) {
            // aggregate every user's activity to this blah
            AggregationOutput output = blahAggregation(blahId);

            // if the blah doesn't have any activity, skip
            Iterator<DBObject> it = output.results().iterator();
            if (it.hasNext()) {
                blahActiveSet.add(blahId);
                blahActiveCount++;
            }
            else continue;

            // the aggregation is grouped by userId, for each user, compute utility and store
            for (DBObject userBlahAggregation : output.results()) {
                UserBlahInfo userBlahInfo = new UserBlahInfo(userBlahAggregation, blahId);

                // if this is a new user, count it
                if (!userActiveSet.contains(userBlahInfo.userId)) {
                    userActiveSet.add(userBlahInfo.userId.toString());
                    userActiveCount++;
                }

                double util = computeUtility(userBlahInfo);

                // utilityBlah : blahId -> utility
                HashMap<String, Double> utilityBlah = utilityBlahInUser.get(userBlahInfo.userId);
                // if this is the first blah for this user, create the blahId -> utility hashmap for him
                if (utilityBlah == null) {
                    utilityBlah = new HashMap<>();
                    utilityBlah.put(blahId, util);
                    utilityBlahInUser.put(userBlahInfo.userId.toString(), utilityBlah);
                } else {
                    utilityBlah.put(blahId, util);
                }
            }
        }
    }

    private AggregationOutput blahAggregation(String blahId) {
        // match blahId, group by userId, sum activities
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject(DBConstants.UserBlahStats.BLAH_ID, new ObjectId(blahId)));

        BasicDBObject groupFields = new BasicDBObject("_id", "$"+DBConstants.UserBlahStats.USER_ID);
        groupFields.append(DBConstants.UserBlahStats.VIEWS, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.VIEWS));
        groupFields.append(DBConstants.UserBlahStats.OPENS, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.OPENS));
        groupFields.append(DBConstants.UserBlahStats.COMMENTS, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.COMMENTS));
        groupFields.append(DBConstants.UserBlahStats.PROMOTION, new BasicDBObject("$sum", "$"+DBConstants.UserBlahStats.PROMOTION));
        BasicDBObject group = new BasicDBObject("$group", groupFields);

        List<DBObject> pipeline = Arrays.asList(match, group);
        return userBlahStatsCol.aggregate(pipeline);
    }

    private class UserBlahInfo {
        ObjectId blahId;
        ObjectId userId;

        int views;
        int opens;
        int comments;
        int promotion;

        private UserBlahInfo(DBObject userBlahAggregation, String blahId) {
            userId = (ObjectId) userBlahAggregation.get("_id");
            this.blahId = new ObjectId(blahId);

            Integer obj;
            obj = (Integer) userBlahAggregation.get(DBConstants.UserBlahStats.VIEWS);
            views = obj == null ? 0 : obj;
            obj = (Integer) userBlahAggregation.get(DBConstants.UserBlahStats.OPENS);
            opens = obj == null ? 0 : obj;
            obj = (Integer) userBlahAggregation.get(DBConstants.UserBlahStats.COMMENTS);
            comments = obj == null ? 0 : obj;
            obj = (Integer) userBlahAggregation.get(DBConstants.UserBlahStats.PROMOTION);
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

    private void indexActiveBlah() throws Exception {
        activeBlahIdIndexMap = new HashMap<>();
        int idx = 0;
        for (String blahId : blahActiveSet) {
            activeBlahIdIndexMap.put(blahId, idx);
            idx++;
        }
        // check numbers are correct
        if (activeBlahIdIndexMap.size() != blahActiveCount) throw new Exception(" Error: active blah number inconsistent!");
    }

    private double[][] getInterestMatrix() {
        // default set to 0
        // only use active blah to do clustering
        data = new double[userCount][blahActiveCount];

        // for each user
        for (String userId : utilityBlahInUser.keySet()) {
            // put user-blah utility into vectors
            // utilityBlah : (blahId -> utility)
            HashMap<String, Double> utilityBlah = utilityBlahInUser.get(userId);
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
        System.out.print("Assigning trivial clustering...");

        numCohort = 1;
        cluster = new ArrayList[userCount];

        // assign all users to the same cohort
        for (int u = 0; u < userCount; u++) {
            cluster[u] = new ArrayList<>();
            cluster[u].add(0);
        }
        System.out.println("done");
    }

    private void randomClustering() {
        System.out.print("Assigning random clustering...");
        cluster = new ArrayList[userCount];
        Random rand = new Random();
        for (int u = 0; u < userCount; u++) {
            cluster[u] = new ArrayList<>();
            for (int i = 0; i < numCohort; i++) {
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

    private void kmeansClustering() throws Exception {
        // k-means to cluster users
        int[] kmeansResult;
        kmeansResult = KMeansClustering.run(data, numCohort);
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
    }

    private void outputResearchFiles() {
        //long version = new Date().getTime() / 1000;
        String version = "";
        String dataFileName = "research/" + version + "data_" + groupName.toLowerCase().replace(' ','_') + ".csv";

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
        cohortIndexIdMap = new String[numCohort];
        for (int c = 0; c < numCohort; c++) {
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
        for (int c = 0; c < numCohort; c++) {
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

    // map cohorts to previous generation of cohorts
    // not used for now
    private void buildCohortMapping() {
        // get userId list for each cohort in previous generation
        HashMap<String, List<String>> prevUserPerCohort = new HashMap<String, List<String>>();
        // get previous generation id
        DBObject groupDoc = groupsCol.findOne(new BasicDBObject("_id", new ObjectId(groupId)));
        String prevGenId = (String) groupDoc.get("CG");
        // get previous generation cohortId set
        DBObject cohortGens = (DBObject)groupDoc.get("CHG");
        DBObject genDoc = (DBObject) cohortGens.get(prevGenId);
        DBObject cohortInfo = (DBObject) genDoc.get("CHI");
        Set<String> prevCohortIdSet = cohortInfo.keySet();
        // get previous generation cohort-ser map
        for (String cohorId : prevCohortIdSet) {
            BasicDBObject cohortDoc = (BasicDBObject) cohortInfoCol.findOne(new BasicDBObject("_id", new ObjectId(cohorId)));
            prevUserPerCohort.put(cohorId, (List<String>) cohortDoc.get("U"));
        }
        // map each new cohort to one of the cohort in previous generation, or null
        for (String cohortId : userPerCohort.keySet()) {
            List<String> userIdList = userPerCohort.get(cohortId);
            Set<String> userIdSet = new HashSet<String>(userIdList);
            boolean foundParent = false;
            for (String prevCohortId : prevUserPerCohort.keySet()) {
                List<String> prevUserIdList = prevUserPerCohort.get(prevCohortId);
                int common = countCommonUser(userIdSet, prevUserIdList);
                if ((double)common / userIdList.size() > sameCohortThreshold && (double)common / prevUserIdList.size() > sameCohortThreshold) {
                    cohortMapping.put(cohortId, prevCohortId);
                    foundParent = true;
                    break;
                }
            }
            if (!foundParent) {
                cohortMapping.put(cohortId, null);
            }
        }
    }

    private int countCommonUser(Set<String> setA, List<String> B) {
        int count = 0;
        for (String b : B) {
            if (setA.contains(b)) {
                count++;
            }
        }
        return count;
    }

    private void outputCohortToMongo() {

        // insert cohort information into infodb.cohortInfo
        insertCohortInfo();

        // insert cohort generation info into infodb.generationInfo
        ObjectId generationIdObj = insertGenerationInfo();

        // write user's cohort info into infodb.usreGroupInfo "next generation cohort"
        updateUserNextCohortInfo();

        // write group next generation Id
        // the ID will be copied to "current generation id" after first set of inboxes are generated
        updateGroupNextGenId(generationIdObj);

        // notice, update userGroupInfo.CH after the first set of inboxes are generated by inboxer
        if (OUTPUT_COHORT_INFO_FOR_TEST) {
            updateUserCohortInfo();
        }
    }

    private void insertCohortInfo() {
        System.out.print("Writing cohort information to database...");

        for (String cohortId : userPerCohort.keySet()) {
            List<String> userIdList = userPerCohort.get(cohortId);

            BasicDBObject cohortInfo = new BasicDBObject();
            cohortInfo.put(DBConstants.CohortInfo.ID, new ObjectId(cohortId));
            cohortInfo.put(DBConstants.CohortInfo.NUM_USERS, (long)userIdList.size());
            // we could add user list for each cohort, but it this necessary?
//            cohortInfo.put(DBConstants.CohortInfo.USER_LIST, convertIdList(userIdList));

            cohortInfoCol.insert(cohortInfo);
        }
        System.out.println("done");
    }

    private ObjectId insertGenerationInfo() {
        System.out.print("Writing generation information to database...");

        BasicDBObject cohortInfoDoc = new BasicDBObject();
        for (String cohortId : cohortIndexIdMap) {
            BasicDBObject defaultInbox = new BasicDBObject(DBConstants.GenerationInfo.FIRST_INBOX, -1L);
            defaultInbox.append(DBConstants.GenerationInfo.LAST_INBOX, -1L);
            cohortInfoDoc.put(cohortId, defaultInbox); // cohortId as keys
        }
        ObjectId generationIdObj = new ObjectId();
        BasicDBObject generationDoc = new BasicDBObject(DBConstants.GenerationInfo.ID, generationIdObj);
        generationDoc.put(DBConstants.GenerationInfo.CREATE_TIME, new Date());
        generationDoc.put(DBConstants.GenerationInfo.GROUP_ID, new ObjectId(groupId));
        generationDoc.put(DBConstants.GenerationInfo.COHORT_INFO, cohortInfoDoc);
        generationInfoCol.insert(generationDoc);

        System.out.println("done");

        System.out.println("cohort generation id : " + generationIdObj.toString());
        for (String cohortId : cohortIndexIdMap) {
            System.out.println("\tcohort id : " + cohortId);
        }

        return generationIdObj;
    }

    private void updateUserNextCohortInfo() {
        System.out.print("Writing next generation cohort information to userGroupInfo ...");

        for (String userId : cohortPerUser.keySet()) {
            List<String> cohortIdList = cohortPerUser.get(userId);

            BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, new ObjectId(userId)).append(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
            BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConstants.UserGroupInfo.NEXT_COHORT_LIST, convertIdList(cohortIdList)));
            userGroupInfoCol.update(query, setter);
        }
        System.out.println("done");
    }

    private void updateGroupNextGenId(ObjectId generationIdObj) {
        System.out.print("Writing next generation id into group : " + groupId + " ...");
        BasicDBObject query = new BasicDBObject(DBConstants.Groups.ID, new ObjectId(groupId));
        BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConstants.Groups.NEXT_GENERATION, generationIdObj));
        groupsCol.update(query, setter);
        System.out.println("done");
    }

    private void updateUserCohortInfo() {
        System.out.print("Writing cohort information to userGroupInfo for testing...");

        for (String userId : cohortPerUser.keySet()) {
            List<String> cohortIdList = cohortPerUser.get(userId);

            BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.USER_ID, new ObjectId(userId)).append(DBConstants.UserGroupInfo.GROUP_ID, new ObjectId(groupId));
            BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject(DBConstants.UserGroupInfo.CURRENT_COHORT_LIST, convertIdList(cohortIdList)));
            userGroupInfoCol.update(query, setter);
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

    private void produceStrengthTask() throws Exception {
        initializeQueue();

        System.out.print("Producing strength computation task...");

        BasicDBObject task = new BasicDBObject();
        task.put(AzureConstants.StrengthTask.TYPE, AzureConstants.StrengthTask.COMPUTE_ALL_STRENGTH);
        task.put(AzureConstants.StrengthTask.GROUP_ID, groupId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        strengthTaskQueue.addMessage(message);

        System.out.println("done");
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

    private void writeFakeBlahCohortStrength() {
        System.out.print("Writing fake cohort-strength to blahs...");
        DBCursor cursor = blahInfoCol.find(new BasicDBObject("G", groupId));

        while (cursor.hasNext()) {
            BasicDBObject blah = (BasicDBObject) cursor.next();
            BasicDBObject cohortStrength = (BasicDBObject) blah.get("CHS");
            if (cohortStrength == null) {
                cohortStrength = new BasicDBObject();
                blah.put("CHS", cohortStrength);
            }
            for (String cohortId : cohortIndexIdMap) {
                cohortStrength.put(cohortId, Math.random()/2);
            }
            blahInfoCol.save(blah);
        }
        System.out.println("done");
    }
}