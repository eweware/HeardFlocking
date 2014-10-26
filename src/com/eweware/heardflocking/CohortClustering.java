package com.eweware.heardflocking;

import com.mongodb.*;
import org.bson.types.ObjectId;

import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 10/10/14.
 */
public class CohortClustering extends TimerTask{

    public static void main(String[] args) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set time to run
//        cal.set(Calendar.HOUR_OF_DAY, 20);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // set period
        int periodHours = 24 * 3;

        System.out.println("CohortClustering set to run once for every " + periodHours + " hours, starting at "  + cal.getTime().toString());

        timer.schedule(new CohortClustering(), cal.getTime(), TimeUnit.HOURS.toMillis(periodHours));
    }

    private final boolean CLUSTERING_RESEARCH = false;
    private final boolean FAKE_CLUSTERING = true;
    private final boolean OUTPUT_COHORT_INFO_FOR_TEST = true;

    // mongo server and databases
    MongoClient mongoClient;
    DB userDB;
    DB blahDB;
    DB infoDB;
    DBCollection groupsCol;
    DBCollection userGroupsCol;
    DBCollection blahsCol;
    DBCollection userBlahInfoCol;
    DBCollection cohortInfoCol;
    DBCollection generationInfoCol;
    DBCollection userGroupInfoCol;

    HashMap<String, String> groups;
    String groupId;
    String groupName;

    // weights for user-blah interest
    double wO = 1.0;
    double wC = 5.0;
    double wP = 10.0;

    // assumed number of cohort for K-means clustering
    int numCohortKMeans = 4;

    // blahIdIndexMap : blahId -> vectorIndex
    HashMap<String, Integer> blahIdIndexMap;
    int blahCount; // #blah in the group

    // userIdIndexMapInGroup : groupId -> (userId -> vectorIndex)
    HashMap<String, Integer> userIdIndexMap;
    int userCount; // #user in the group

    // interestBlahInUser : userId -> (blahId -> interest)
    HashMap<String, HashMap<String, Double>> interestBlahInUser;
    int blahActiveCount; // #blah with any activity in the group
    int userActiveCount; // #user with any activity in the group

    // mapping cohorts between two generations, new cohortId -> old cohortId
    HashMap<String, String> cohortMapping;
    final double sameCohortThreshold = 0.9;

    // user-blah interest matrix
    double[][] data;

    // cohort clustering result
    HashMap<String, List<String>> userPerCohort; // cohortId -> List of userId
    List<String> cohortIdList; // list of cohortId for this group

    @Override
    public void run() {
        try {
            System.out.println();
            System.out.println("########## Start scheduled clustering task ##########  " + new Date());
            System.out.println();

            initializeMongoDB();
            getGroups();

            for (String gid : groups.keySet()) {

                System.out.print("Check need for clustering group '" + groups.get(gid) + "' id : " + gid + " ...");
                if (!checkNeedClustering(gid)) {
                    System.out.println("NO");
                    continue;
                }
                else {
                    System.out.println("YES");
                }

                groupId = gid;
                groupName = groups.get(groupId);


                initializeVariables();

                System.out.println("Start cohort clustering for '" + groupName + "' id : " + groupId);

                // count number of blahs in this group
                // assign a vector index for each blah
                countAndIndexBlah();
                System.out.println("#blah : " + blahCount);

                // count number of users in this group
                // assign a index for each user
                countAndIndexUser();
                System.out.println("#users : " + userCount);

                // compute user-blah interest
                computeInterestAll();
                System.out.println("#blah active : " + blahActiveCount);
                System.out.println("#user active : " + userActiveCount);

                // convert interestBlahInUser into matrix, using blahIdIndexMap
                getInterestMatrix();

                // to do separate research on the data, output to files
                if (CLUSTERING_RESEARCH) {
                    System.out.println("Writing data to local files for research, no clustering is done.");
                    outputResearchFiles();
                    return;
                }

                ArrayList<Integer>[] cluster;
                if (FAKE_CLUSTERING) {
                    System.out.println("Assigning random clustering...");
                    cluster = new ArrayList[userCount];
                    Random rand = new Random();
                    for (int u = 0; u < userCount; u++) {
                        cluster[u] = new ArrayList<Integer>();
                        for (int i = 0; i < numCohortKMeans; i++) {
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
                } else {
                    // k-means to cluster users
                    int[] kmeansResult;
                    kmeansResult = KMeansClustering.run(data, numCohortKMeans);
                    if (kmeansResult == null) {
                        System.out.println("Error : k-means return null");
                        return;
                    }
                    // turn k-means result into cluster form
                    cluster = new ArrayList[kmeansResult.length];
                    for (int u = 0; u < kmeansResult.length; u++) {
                        cluster[u] = new ArrayList<Integer>();
                        cluster[u].add(kmeansResult[u]);
                    }
                }

                // put result into cohort hashmap
                produceCohortHashMap(cluster, numCohortKMeans);

                // find counterpart of new cohorts in previous generation
                //buildCohortMapping();

                // write cohort info into mongo
                outputCohortToMongo();

                //writeFakeBlahCohortStrength();

                System.out.println();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeMongoDB() throws UnknownHostException {
        System.out.print("Initializing MongoDB connection...");

        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DB_SERVER_PORT);

        userDB = mongoClient.getDB("userdb");
        blahDB = mongoClient.getDB("blahdb");
        infoDB = mongoClient.getDB("infodb");

        groupsCol = userDB.getCollection("groups");
        userGroupsCol = userDB.getCollection("usergroups");
        userBlahInfoCol = userDB.getCollection("userBlahInfo");
        blahsCol = blahDB.getCollection("blahs");

        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");

        System.out.println("done");
    }

    private void getGroups() {
        Cursor cursor = groupsCol.find();
        groups = new HashMap<String, String>();
        while (cursor.hasNext()) {
            BasicDBObject group = (BasicDBObject) cursor.next();
            groups.put(group.getObjectId("_id").toString(), group.getString("N"));
        }
        cursor.close();
    }

    private boolean checkNeedClustering(String id) {
        //TODO add condition to re-cluster cohorts for this group
        return true;
    }

    private void initializeVariables() {
        blahIdIndexMap = new HashMap<String, Integer>();
        blahCount = 0;
        userIdIndexMap = new HashMap<String, Integer>();
        userCount = 0;

        interestBlahInUser = new HashMap<String, HashMap<String, Double>>();
        blahActiveCount = 0;
        userActiveCount = 0;

        cohortMapping = new HashMap<String, String>();

        userPerCohort = new HashMap<String, List<String>>();
        cohortIdList = new ArrayList<String>();
    }

    private void countAndIndexBlah() {
        BasicDBObject queryGroup = new BasicDBObject("G", groupId);
        DBCursor cursor = blahsCol.find(queryGroup);
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            String blahId = obj.getObjectId("_id").toString();
            // count blah and assign index for each blah
            blahIdIndexMap.put(blahId, blahCount);
            blahCount++;
        }
        cursor.close();
    }

    private void countAndIndexUser() {
        BasicDBObject queryGroup = new BasicDBObject("G", groupId);
        DBCursor cursor = userGroupsCol.find(queryGroup);
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            // skip non-active user
            String status = (String) obj.get("S");
            if (!status.equals("A")) continue;

            String userId = (String) obj.get("U");
            // count user and assign index for each user
            userIdIndexMap.put(userId, userCount);
            userCount++;
        }
        cursor.close();
    }

    private void computeInterestAll() {
        BasicDBObject queryGroup = new BasicDBObject("G", groupId);
        DBCursor cursor = userBlahInfoCol.find(queryGroup);

        HashSet<String> blahActiveSet = new HashSet<String>();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            String userId = (String) obj.get("U");
            String blahId = (String) obj.get("B");

            // interestBlah : blahId -> interest
            HashMap<String, Double> interestBlah = interestBlahInUser.get(userId);
            if (interestBlah == null) {
                interestBlahInUser.put(userId, new HashMap<String, Double>());
                interestBlah = interestBlahInUser.get(userId);
                userActiveCount++;
            }

            // insert user-blah interest
            interestBlah.put(blahId, computeInterest(obj));
            blahActiveSet.add(blahId);
        }
        blahActiveCount = blahActiveSet.size();
        cursor.close();
    }

    private double computeInterest(BasicDBObject userBlah) {
        // open
        Long O = (Long)userBlah.get("O");
        O = O == null ? 0 : O;

        // comment
        Long C = (Long)userBlah.get("C");
        C = C == null ? 0 : C;

        // promote
        // P > 0 upvote:    more people should see this
        // P < 0 downvode:  fewer people should see this
        Long P = (Long)userBlah.get("P");
        P = P == null ? 0 : P;

        // compute interest strength
        return O * wO + C * wC + P * wP;
    }

    private double[][] getInterestMatrix() {
        // default set to 0
        data = new double[userCount][blahCount];

        // for each user
        for (String userId : interestBlahInUser.keySet()) {
            // put user-blah interest into vectors
            // interestBlah : (blahId -> interest)
            HashMap<String, Double> interestBlah = interestBlahInUser.get(userId);
            // for each blah that the user interacted with
            for (String blahId : interestBlah.keySet()) {
                // check data consistency:
                // there may be user in userdb.userBlahInfo but not in userdb.usergroups
                // or blah in userdb.userBlahInfo but not in blahdb.blahs
                // ignore these users and blahs for now
                if (userIdIndexMap.get(userId) != null && blahIdIndexMap.get(blahId) != null)
                    data[userIdIndexMap.get(userId)][blahIdIndexMap.get(blahId)] = interestBlah.get(blahId);
            }
        }
        return data;
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
    private void produceCohortHashMap(ArrayList<Integer>[] cluster, int numOfCohort) {
        // generate cohortIndex->cohortId map
        String[] cohortIndexIdMap = new String[numOfCohort];
        for (int c = 0; c < numOfCohort; c++) {
            cohortIndexIdMap[c] = new ObjectId().toString();
            cohortIdList.add(cohortIndexIdMap[c]);
        }

        // cohortId -> list of userId
        for (int c = 0; c < numOfCohort; c++) {
            userPerCohort.put(cohortIndexIdMap[c], new ArrayList<String>());
        }

        // add userId
        for (String userId : userIdIndexMap.keySet()) {
            int userIndex = userIdIndexMap.get(userId);
            for (int cohortIndex : cluster[userIndex]) {
                String cohortId = cohortIndexIdMap[cohortIndex];
                userPerCohort.get(cohortId).add(userId);
            }
        }
    }

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
            DBObject cohortDoc = (DBObject) cohortInfoCol.findOne(new BasicDBObject("_id", new ObjectId(cohorId)));
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
        System.out.print("Writing cohort information to database...");

        // insert infodb.cohortInfo
        for (String cohortId : userPerCohort.keySet()) {
            List<String> userIdList = userPerCohort.get(cohortId);
            BasicDBObject cohortInfo = new BasicDBObject();
            cohortInfo.put("_id", new ObjectId(cohortId));
            cohortInfo.put("N", userIdList.size());
//            cohortInfo.put("G", groupName);
            cohortInfo.put("U", userIdList);
            cohortInfoCol.insert(cohortInfo);
        }

        System.out.println("done");

        System.out.print("Writing generation information to database...");
        // insert cohort generation info into infodb.generationInfo
        BasicDBObject cohortInfoDoc = new BasicDBObject();
        for (String cohortId : cohortIdList) {
            cohortInfoDoc.put(cohortId, new BasicDBObject());
        }
        ObjectId generationObjId = new ObjectId();
        BasicDBObject generationDoc = new BasicDBObject("_id", generationObjId);
        generationDoc.put("c", new Date());
        generationDoc.put("G", groupId);
        generationDoc.put("CHI", cohortInfoDoc);
        generationInfoCol.insert(generationDoc);

        System.out.println("done");

        System.out.println("cohort generation id : " + generationObjId.toString());
        for (String cohortId : cohortIdList) {
            System.out.println("\tcohort id : " + cohortId);
        }

        // notice, update userGroupInfo.CH and userdb.groups.CG after the first set of inboxes are generated by inboxer
        if (OUTPUT_COHORT_INFO_FOR_TEST) {
            System.out.println("Writing cohort information to usergroups and groups immediately for testing...");
            // write group current generation info
            BasicDBObject query = new BasicDBObject("_id", new ObjectId(groupId));
            BasicDBObject setter = new BasicDBObject("$set", new BasicDBObject("CG", generationObjId.toString()));
            groupsCol.update(query, setter);

            // write user's cohort info
            HashMap<String, List<String>> userCohortInfo = new HashMap<String, List<String>>();
            for (String cohortId : userPerCohort.keySet()) {
                List<String> userIdList = userPerCohort.get(cohortId);
                for (String userId : userIdList) {
                    if (userCohortInfo.containsKey(userId)) {
                        userCohortInfo.get(userId).add(cohortId);
                    } else {
                        ArrayList<String> list = new ArrayList<String>();
                        list.add(cohortId);
                        userCohortInfo.put(userId, list);
                    }
                }
            }

            for (String userId : userCohortInfo.keySet()) {
                query = new BasicDBObject("U", userId).append("G", groupId);
                setter = new BasicDBObject("$set", new BasicDBObject("CH", userCohortInfo.get(userId)));
                userGroupInfoCol.update(query, setter);
            }
            System.out.println("done");
        }
    }

    private void writeFakeBlahCohortStrength() {
        System.out.print("Writing fake cohort-strength to blahs...");
        DBCursor cursor = blahsCol.find(new BasicDBObject("G", groupId));

        while (cursor.hasNext()) {
            BasicDBObject blah = (BasicDBObject) cursor.next();
            BasicDBObject cohortStrength = (BasicDBObject) blah.get("CHS");
            if (cohortStrength == null) {
                cohortStrength = new BasicDBObject();
                blah.put("CHS", cohortStrength);
            }
            for (String cohortId : cohortIdList) {
                cohortStrength.put(cohortId, Math.random()/2);
            }
            blahsCol.save(blah);
        }
        System.out.println("done");
    }
}
