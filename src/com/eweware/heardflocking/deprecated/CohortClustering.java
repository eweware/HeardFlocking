package com.eweware.heardflocking.deprecated;

import com.eweware.heardflocking.deprecated.KMeansClustering;
import com.mongodb.*;
import org.bson.types.ObjectId;

import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by weihan on 10/10/14.
 */
public class CohortClustering {
    public CohortClustering(MongoClient mongo, String id) {
        this.groupId = id;
        mongoClient = mongo;
    }

    private final boolean research = true;

    // mongo server and databases
    final MongoClient mongoClient;
    DB userDB;
    DB blahDB;
    DBCollection groupsColl;
    DBCollection userGroupsColl;
    DBCollection blahsColl;
    DBCollection userBlahInfoColl;
    DBCollection cohortColl;

    final String groupId;
    String groupName;

    // weights for user-blah interest
    double wO = 1.0;
    double wC = 5.0;
    double wP = 10.0;

    // assumed number of cohort for K-means clustering
    int numCohortKMeans = 2;

    // blahIdIndexMap : blahId -> vectorIndex
    HashMap<String, Integer> blahIdIndexMap = new HashMap<String, Integer>();
    int blahCount = 0; // #blah in the group

    // userIdIndexMapInGroup : groupId -> (userId -> vectorIndex)
    HashMap<String, Integer> userIdIndexMap = new HashMap<String, Integer>();
    int userCount = 0; // #user in the group

    // interestBlahInUser : userId -> (blahId -> interest)
    HashMap<String, HashMap<String, Double>> interestBlahInUser = new HashMap<String, HashMap<String, Double>>();
    int blahActiveCount = 0; // #blah with any activity in the group
    int userActiveCount = 0; // #user with any activity in the group

    // mapping cohorts between two generations, new cohortId -> old cohortId
    HashMap<String, String> cohortMapping = new HashMap<String, String>();
    double sameCohortThreshold = 0.9;

    // user-blah interest matrix
    double[][] data;

    // cohort clustering result
    HashMap<String, List<String>> userPerCohort = new HashMap<String, List<String>>(); // cohortId -> List of userId
    List<String> cohortIdList = new ArrayList<String>(); // list of cohortId for this group

    public void execute() throws UnknownHostException {
        // open MongoDB connection
        initDatabase();

        // get group name
        getGroupName();
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
        if (research) {
            System.out.println("Writing data to local files for research, no clustering is done.");
            outputResearchFiles();
            return;
        }

        // k-means to cluster users
        int[] kmeansResult = KMeansClustering.run(data, numCohortKMeans);
        if (kmeansResult == null) {
            System.out.println("Error : k-means return null");
            return;
        }
        // turn k-means result into cluster form
        ArrayList<Integer>[] cluster = new ArrayList[kmeansResult.length];
        for (int u = 0; u < kmeansResult.length; u++) {
            cluster[u] = new ArrayList<Integer>();
            cluster[u].add(kmeansResult[u]);
        }

        // put result into cohort hashmap
        produceCohortHashMap(cluster, numCohortKMeans);

        // find counterpart of new cohorts in previous generation
        buildCohortMapping();

        // write cohort info into mongo
        outputCohortToMongo();

        //writeFakeBlahCohortStrength();

        System.out.println();
    }

    private void initDatabase() throws UnknownHostException {
        userDB = mongoClient.getDB("userdb");
        blahDB = mongoClient.getDB("blahdb");
        groupsColl = userDB.getCollection("groups");
        blahsColl = blahDB.getCollection("blahs");
        userGroupsColl = userDB.getCollection("usergroups");
        userBlahInfoColl = userDB.getCollection("userBlahInfo");
        cohortColl = userDB.getCollection("cohorts");
    }

    private void getGroupName() {
        BasicDBObject obj = (BasicDBObject) groupsColl.findOne(new BasicDBObject("_id", new ObjectId(groupId)));
        groupName = obj.getString("N");
    }

    private void countAndIndexBlah() {
        BasicDBObject queryGroup = new BasicDBObject("G", groupId);
        DBCursor cursor = blahsColl.find(queryGroup);
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
        DBCursor cursor = userGroupsColl.find(queryGroup);
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
        DBCursor cursor = userBlahInfoColl.find(queryGroup);

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
        DBObject groupDoc = groupsColl.findOne(new BasicDBObject("_id", new ObjectId(groupId)));
        String prevGenId = (String) groupDoc.get("CG");
        // get previous generation cohortId set
        DBObject cohortGens = (DBObject)groupDoc.get("CHG");
        DBObject genDoc = (DBObject) cohortGens.get(prevGenId);
        DBObject cohortInfo = (DBObject) genDoc.get("CHI");
        Set<String> prevCohortIdSet = cohortInfo.keySet();
        // get previous generation cohort-ser map
        for (String cohorId : prevCohortIdSet) {
            DBObject cohortDoc = (DBObject) cohortColl.findOne(new BasicDBObject("_id", new ObjectId(cohorId)));
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

        // insert userdb.cohorts
        //  _id: ObjectId   cohortId
        //  U:   String[]   userId[]
        for (String cohortId : userPerCohort.keySet()) {
            List<String> userIdList = userPerCohort.get(cohortId);
            cohortColl.insert(
                    new BasicDBObject("_id", new ObjectId(cohortId)).append("N", userIdList.size()).append("G", groupName).append("U", userIdList).append("P", cohortMapping.get(cohortId))
            );
        }
        //cohortBuilder.execute();

        // insert cohort generation info into userdb.groups
        BasicDBObject groupEntry = new BasicDBObject("_id", new ObjectId(groupId));
        String generationId = new ObjectId().toString();
        BasicDBObject cohortInfoDoc = new BasicDBObject();
        for (String cohortId : cohortIdList) {
            BasicDBObject defaultInboxNumberDoc = new BasicDBObject("F", -1).append("L", -1).append("FS", -1).append("LS", -1);
            cohortInfoDoc.append(cohortId, defaultInboxNumberDoc);
        }
        BasicDBObject generationObj = new BasicDBObject("c", new Date()).append("CHI", cohortInfoDoc);
        groupsColl.update(groupEntry, new BasicDBObject("$set", new BasicDBObject("CHG."+generationId, generationObj)));

        System.out.println("done");

        System.out.println("cohort generation id : " + generationId);
        for (String cohortId : cohortIdList) {
            System.out.println("\tcohort id : " + cohortId);
        }
    }

    private void writeFakeBlahCohortStrength() {
        System.out.print("Writing fake cohort-strength to blahs...");
        DBCursor cursor = blahsColl.find(new BasicDBObject("G", groupId));

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
            blahsColl.save(blah);
        }
        System.out.println("done");
    }
}
