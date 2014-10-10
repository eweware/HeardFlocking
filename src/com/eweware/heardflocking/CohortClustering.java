package com.eweware.heardflocking;

import com.mongodb.*;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by weihan on 10/10/14.
 */
public class CohortClustering {
    public CohortClustering(String server, int port, String groupId) {
        this.groupId = groupId;
        mongoServer = server;
        mongoServerPort = port;
    }
    // mongo server and databases
    final String mongoServer;
    final int mongoServerPort;
    MongoClient mongoClient;
    DB userDB;
    DB blahDB;
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

    // cohort clustering result
    HashMap<String, List<String>> userPerCohort = new HashMap<String, List<String>>(); // cohortId -> List of userId
    List<String> cohortList = new ArrayList<String>(); // list of cohortId for this group

    public void run() throws UnknownHostException {
        // open MongoDB connection
        initDatabase();

        // get group name
        getGroupName();
        System.out.println("Start cohort clustering for");
        System.out.println("\t" + groupName + " : " + groupId);

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
        double[][] data = getInterestMatrix();

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

        // write cohort info into mongo
        outputCohortToMongo();
    }

    private void initDatabase() throws UnknownHostException {
        mongoClient = new MongoClient(mongoServer, mongoServerPort);
        userDB = mongoClient.getDB("userdb");
        blahDB = mongoClient.getDB("blahdb");
    }

    private void getGroupName() {
        DBCollection groupsColl = userDB.getCollection("groups");
        BasicDBObject obj = (BasicDBObject) groupsColl.findOne(new BasicDBObject("_id", new ObjectId(groupId)));
        groupName = obj.getString("N");
    }

    private void countAndIndexBlah() {
        DBCollection blahsColl = blahDB.getCollection("blahs");
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
        DBCollection userGroupsColl = userDB.getCollection("usergroups");
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
        DBCollection userBlahInfoColl = userDB.getCollection("userBlahInfo");
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
        double[][] data = new double[userCount][blahCount];

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

    // cluster input : userIndex -> list of clusterIndex
    private void produceCohortHashMap(ArrayList<Integer>[] cluster, int numOfCohort) {
        // generate cohortIndex->cohortId map
        String[] cohortIndexIdMap = new String[numOfCohort];
        for (int c = 0; c < numOfCohort; c++) {
            cohortIndexIdMap[c] = new ObjectId().toString();
            cohortList.add(cohortIndexIdMap[c]);
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

    private void outputCohortToMongo() {
        System.out.print("Writing cohort information to database...");

        // insert userdb.cohorts
        //  _id: ObjectId   cohortId
        //  U:   String[]   userId[]
        DBCollection cohortColl = userDB.getCollection("cohorts");

        for (String cohortId : userPerCohort.keySet()) {
            List<String> userIdList = userPerCohort.get(cohortId);
            cohortColl.insert(
                    new BasicDBObject("_id", new ObjectId(cohortId)).append("N", userIdList.size()).append("G", groupName).append("U", userIdList)
            );
        }
        //cohortBuilder.execute();

        // insert cohort generation info into userdb.groups
        DBCollection groupsColl = userDB.getCollection("groups");

        BasicDBObject groupEntry = new BasicDBObject("_id", new ObjectId(groupId));
        BasicDBObject generationObj = new BasicDBObject("_id", new ObjectId()).append("c", new Date()).append("CH", cohortList);
        groupsColl.update(groupEntry, new BasicDBObject("$push", new BasicDBObject("CHG", generationObj)));

        System.out.println("done");
    }
}
