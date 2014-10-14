package com.eweware.heardflocking;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Timer;

/**
 * Created by weihan on 10/10/14.
 */
public class Main {
    public static void main(String[] args) throws UnknownHostException {
        Timer timer = new Timer();
        ScheduledClusteringTask clusteringTask = new ScheduledClusteringTask();
        timer.schedule(clusteringTask, 0, 10 * MSEC_PER_MIN);
    }

    private static final long MSEC_PER_SEC = 1000;
    private static final long MSEC_PER_MIN = 60 * MSEC_PER_SEC;
    private static final long MSEC_PER_HOUR = 60 * MSEC_PER_MIN;

}
