package com.eweware.heardflocking;

import java.net.UnknownHostException;
import java.util.*;

import com.mongodb.*;
import org.bson.types.ObjectId;

/**
 * Created by weihan.kong on 10/7/2014.
 */
public class CohortClustering {
    public static void main(String[] args) throws UnknownHostException {
        //KMeansClustering.test();
        instance.run();
    }
    static private CohortClustering instance = new CohortClustering();

    private void run() throws UnknownHostException {
        // open mongo connection
        initDatabase();

        // get group names
        getGroupNames();

        for (String id : groupNames.keySet()) { System.out.println(id + " : " + groupNames.get(id)); }

        // count number of blahs in each group
        // assign a vector index for each blah
        countAndIndexBlahInGroup();

        System.out.println("#blah");
        for (String groupId : countBlahInGroup.keySet()) {
            System.out.println(groupId + " : " + countBlahInGroup.get(groupId) + "\t" + groupNames.get(groupId));
        }


        // count number of users in each group
        // assign a index for each user
        countAndIndexUserInGroup();

        System.out.println("#users");
        for (String groupId : countUserInGroup.keySet()) {
            System.out.println(groupId + " : " + countUserInGroup.get(groupId) + "\t" + groupNames.get(groupId));
        }


        // compute user-blah interest
        computeInterestAll();

        /*
        System.out.println("#blah - check");
        for (String groupId : countBlahInGroupCheck.keySet()) {
            System.out.println(groupId + " : " + countBlahInGroupCheck.get(groupId) + "\t" + groupNames.get(groupId));
        }
        */


        // for each group, do k-means
        for (String groupId : groupNames.keySet()) {
            // convert interestBlahInUserInGroup into vectors, using blahIdIndexMapInGroup
            double[][] data = getInterestVectors(groupId);

            // k-means to cluster users
            System.out.println("Clustering for : " + groupNames.get(groupId));
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
            produceCohortHashMap(groupId, cluster, numCohortKMeans);
        }


        // write cohort info into mongo
        outputCohortToMongo();
    }

    // mongo server and databases
    final String mongoServer = "localhost";
    final int mongoServerPort = 21191;
    MongoClient mongoClient;
    DB userDB;
    DB blahDB;

    // weights for user-blah interest
    double wO = 1.0;
    double wC = 5.0;
    double wP = 10.0;

    // assumed number of cohort for K-means clustering
    int numCohortKMeans = 2;

    HashMap<String, String> groupNames = new HashMap<String, String>();

    // interestBlahInUserInGroup : groupId -> (userId -> (blahId -> interest))
    HashMap<String, HashMap<String, HashMap<String, Double>>>
            interestBlahInUserInGroup = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

    // blahIdIndexMapInGroup : groupId -> (blahId -> vectorIndex)
    HashMap<String, HashMap<String, Integer>>
            blahIdIndexMapInGroup = new HashMap<String, HashMap<String, Integer>>();
    // countBlahInGroup : groupId -> number of blahs in the group
    HashMap<String, Integer>
            countBlahInGroup = new HashMap<String, Integer>();

    /* check for consistency of blah information between blahdb.blahs and userdb.userBlahInfo
    HashMap<String, HashMap<String, Integer>>
            blahIdIndexMapInGroupCheck = new HashMap<String, HashMap<String, Integer>>();
    HashMap<String, Integer>
            countBlahInGroupCheck = new HashMap<String, Integer>();
    */

    // userIdIndexMapInGroup : groupId -> (userId -> vectorIndex)
    HashMap<String, HashMap<String, Integer>>
            userIdIndexMapInGroup = new HashMap<String, HashMap<String, Integer>>();
    // countUserInGroup : groupId -> number of users in the group
    HashMap<String, Integer>
            countUserInGroup = new HashMap<String, Integer>();

    // cohort clustering result
    // groupId -> (userId -> cohortId[])
    HashMap<String, HashMap<String, List<String>>> cohortPerUserInGroup = new HashMap<String, HashMap<String, List<String>>>();
    // groupId -> (cohortId -> userId[])
    HashMap<String, HashMap<String, List<String>>> userPerCohortInGroup = new HashMap<String, HashMap<String, List<String>>>();

    // list of cohortId for each group
    HashMap<String, List<String>> cohortListInGroup = new HashMap<String, List<String>>();

    private void initDatabase() throws UnknownHostException {
        mongoClient = new MongoClient(mongoServer, mongoServerPort);
        userDB = mongoClient.getDB("userdb");
        blahDB = mongoClient.getDB("blahdb");
    }

    private void getGroupNames() {
        DBCollection groupsColl = userDB.getCollection("groups");
        DBCursor cursor = groupsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private void countAndIndexBlahInGroup() {
        DBCollection blahsColl = blahDB.getCollection("blahs");
        DBCursor cursor = blahsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            String groupId = (String) obj.get("G");
            String blahId = obj.getObjectId("_id").toString();

            // count blah in each group
            // blahIdIndexMap : blahId -> vectorIndex
            HashMap<String, Integer> blahIdIndexMap = blahIdIndexMapInGroup.get(groupId);
            if (blahIdIndexMap == null) {
                blahIdIndexMapInGroup.put(groupId, new HashMap<String, Integer>());
                blahIdIndexMap = blahIdIndexMapInGroup.get(groupId);
                countBlahInGroup.put(groupId, 0);
            }
            // generate index for each blah
            if (blahIdIndexMap.get(blahId) == null) {
                int count = countBlahInGroup.get(groupId);
                blahIdIndexMap.put(blahId, count);
                count++;
                countBlahInGroup.put(groupId, count);
            }
        }
        cursor.close();
    }

    private void countAndIndexUserInGroup() {
        DBCollection userGroupsColl = userDB.getCollection("usergroups");
        DBCursor cursor = userGroupsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            String status = (String) obj.get("S");
            if (!status.equals("A")) continue; // only cluster active user

            String groupId = (String) obj.get("G");
            String userId = (String) obj.get("U");

            // count user in each group
            // userIdIndexMap: userId -> index
            HashMap<String, Integer> userIdIndexMap = userIdIndexMapInGroup.get(groupId);
            if (userIdIndexMap == null) {
                userIdIndexMapInGroup.put(groupId, new HashMap<String, Integer>());
                userIdIndexMap = userIdIndexMapInGroup.get(groupId);
                countUserInGroup.put(groupId, 0);
            }

            // generate index for each user
            if (userIdIndexMap.get(userId) == null) {
                int count = countUserInGroup.get(groupId);
                userIdIndexMap.put(userId, count);
                count++;
                countUserInGroup.put(groupId, count);
            }
        }
        cursor.close();
    }

    private void computeInterestAll() {
        DBCollection userBlahInfoColl = userDB.getCollection("userBlahInfo");

        DBCursor cursor = userBlahInfoColl.find();

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            double interest = computeInterest(obj);
            if (interest != 0) {
                //System.out.println(obj);
                //System.out.println(" : " + interest);
            }

            String groupId = (String) obj.get("G");
            String userId = (String) obj.get("U");
            String blahId = (String) obj.get("B");

            // interestBlahInUser: userId -> (blahId -> interest)
            HashMap<String, HashMap<String, Double>> interestBlahInUser = interestBlahInUserInGroup.get(groupId);
            if (interestBlahInUser == null) {
                interestBlahInUserInGroup.put(groupId, new HashMap<String, HashMap<String, Double>>());
                interestBlahInUser = interestBlahInUserInGroup.get(groupId);
            }

            // interestBlah : blahId -> interest
            HashMap<String, Double> interestBlah = interestBlahInUser.get(userId);
            if (interestBlah == null) {
                interestBlahInUser.put(userId, new HashMap<String, Double>());
                interestBlah = interestBlahInUser.get(userId);
            }

            // insert user-blah interest
            interestBlah.put(blahId, computeInterest(obj));

            // count blah in each user, check
            /*
            // blahIdIndexMap : blahId -> vectorIndex
            HashMap<String, Integer> blahIdIndexMap = blahIdIndexMapInGroupCheck.get(groupId);
            if (blahIdIndexMap == null) {
                blahIdIndexMapInGroupCheck.put(groupId, new HashMap<String, Integer>());
                blahIdIndexMap = blahIdIndexMapInGroupCheck.get(groupId);
                countBlahInGroupCheck.put(groupId, 0);
            }
            // generate index for each blah
            if (blahIdIndexMap.get(blahId) == null) {
                int count = countBlahInGroupCheck.get(groupId);
                blahIdIndexMap.put(blahId, count);
                count++;
                countBlahInGroupCheck.put(groupId, count);
            }
            */
        }
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
        Long P = (Long)userBlah.get("P");
        P = P == null ? 0 : P;

        // compute interest strength
        return O * wO + C * wC + P * wP * Math.signum(P); // downvote shows big interest and disagree
        //return O * wO + C * wC + P * wP; // downvote shows no interest or dislike
    }

    private double[][] getInterestVectors(String groupId) {
        // default set to 0
        double[][] data = new double[countUserInGroup.get(groupId)][countBlahInGroup.get(groupId)];

        // interestBlahInUser : userId -> (blahId -> interest)
        HashMap<String, HashMap<String, Double>> interestBlahInUser = interestBlahInUserInGroup.get(groupId);

        // indexBlah : blahId -> index of the blah
        HashMap<String, Integer> indexBlah = blahIdIndexMapInGroup.get(groupId);

        // indexUser : userId -> index of the user
        HashMap<String, Integer> indexUser = userIdIndexMapInGroup.get(groupId);

        // for each user
        for (String userId : userIdIndexMapInGroup.get(groupId).keySet()) {
            // put user-blah interest into vectors
            // interestBlah : (blahId -> interest)
            HashMap<String, Double> interestBlah = interestBlahInUser.get(userId);
            // skip if user has no activity in this group
            if (interestBlah == null) continue;
            // for each blah that the user has interacted with
            for (String blahId : interestBlah.keySet()) {
                if (indexUser.get(userId) != null && indexBlah.get(blahId) != null)
                    data[indexUser.get(userId)][indexBlah.get(blahId)] = interestBlah.get(blahId);
            }
        }
        return data;
    }

    // cluster input : userIndex -> list of clusterIndex
    private void produceCohortHashMap(String groupId, ArrayList<Integer>[] cluster, int numOfCohort) {
        // generate cohortIndex->cohortId map
        String[] cohortIndexIdMap = new String[numOfCohort];
        List<String> cohortList = new ArrayList<String>();
        for (int c = 0; c < numOfCohort; c++) {
            cohortIndexIdMap[c] = new ObjectId().toString();
            cohortList.add(cohortIndexIdMap[c]);
        }
        cohortListInGroup.put(groupId, cohortList);

        // userId -> list of cohortId
        /*
        HashMap<String, Integer> userIdIndexMap = userIdIndexMapInGroup.get(groupId);
        HashMap<String, List<String>> cohortPerUser = new HashMap<String, List<String>>();
        for (String userId : userIdIndexMap.keySet()) {
            ArrayList<String> cohorts = new ArrayList<String>();
            int userIndex = userIdIndexMap.get(userId);
            for (int i = 0; i < cluster[userIndex].size(); i++) {
                int cohortIndex = cluster[userIndex].get(i);
                String cohortId = cohortIndexIdMap[cohortIndex];
                cohorts.add(cohortId);
            }
            cohortPerUser.put(userId, cohorts);
        }
        cohortPerUserInGroup.put(groupId, cohortPerUser);
        */

        // cohortId -> list of userId
        HashMap<String, Integer> userIdIndexMap = userIdIndexMapInGroup.get(groupId);
        HashMap<String, List<String>> userPerCohort = new HashMap<String, List<String>>();
        for (int c = 0; c < numOfCohort; c++) {
            userPerCohort.put(cohortIndexIdMap[c], new ArrayList<String>());
        }
        // add userId
        for (String userId : userIdIndexMap.keySet()) {
            int userIndex = userIdIndexMap.get(userId);
            for (int i = 0; i < cluster[userIndex].size(); i++) {
                int cohortIndex = cluster[userIndex].get(i);
                String cohortId = cohortIndexIdMap[cohortIndex];
                userPerCohort.get(cohortId).add(userId);
            }
        }
        userPerCohortInGroup.put(groupId, userPerCohort);
    }

    private void outputCohortToMongo() {
        System.out.print("Writing cohort information to database...");

        // insert userdb.cohorts
        //  _id: ObjectId   cohortId
        //  U:   String[]   userId[]
        DBCollection cohortColl = userDB.getCollection("cohorts");
        BulkWriteOperation cohortBuilder = cohortColl.initializeUnorderedBulkOperation();

        for (String groupId : userPerCohortInGroup.keySet()) {
            HashMap<String, List<String>> userPerCohort = userPerCohortInGroup.get(groupId);
            for (String cohortId : userPerCohort.keySet()) {
                List<String> userIdList = new ArrayList<String>();
                for (String userId : userPerCohort.get(cohortId)) {
                    userIdList.add(userId);
                }
                cohortBuilder.insert(
                        new BasicDBObject("_id", new ObjectId(cohortId)).append("N", userIdList.size()).append("G", groupNames.get(groupId)).append("U", userIdList)
                );
            }
        }
        cohortBuilder.execute();

        // insert cohort generation info into userdb.groups
        DBCollection groupsColl = userDB.getCollection("groups");

        for (String groupId : groupNames.keySet()) {
            BasicDBObject groupEntry = new BasicDBObject("_id", new ObjectId(groupId));
            List<String> cohortList = cohortListInGroup.get(groupId);
            BasicDBObject generationObj = new BasicDBObject("_id", new ObjectId()).append("c", new Date()).append("CH", cohortList);
            groupsColl.update(groupEntry, new BasicDBObject("$push", new BasicDBObject("CHG", generationObj)));
        }

        System.out.println("done");
    }

}
