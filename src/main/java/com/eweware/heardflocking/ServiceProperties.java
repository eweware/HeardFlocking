package com.eweware.heardflocking;

/**
 * Created by weihan on 11/1/14.
 */
public class ServiceProperties {
    public static boolean TEST_ONLY_TECH;

    public static class CohortMonitor {
        public static int START_HOUR;
        public static int PERIOD_HOURS;
    }

    public static class CohortWorker {
        public static int QUEUE_VISIBLE_TIMEOUT_SECONDS;
        public static long NO_TASK_WAIT_MILLIS;

        public static String CLUSTERING_METHOD;
        public static int NUM_COHORTS;

        public static double WEIGHT_VIEW;
        public static double WEIGHT_OPEN;
        public static double WEIGHT_COMMENT;
        public static double WEIGHT_UPVOTES;
        public static double WEIGHT_DOWNVOTES;
        public static double WEIGHT_COMMENT_UPVOTES;
        public static double WEIGHT_COMMENT_DOWNVOTES;

        public static int RECENT_BLAH_DAYS;
    }

    public static class StrengthMonitor {
        public static int PERIOD_MINUTES;
        public static int RECENT_BLAH_MONTHS;

        public static double BLAH_WEIGHT_VIEW;
        public static double BLAH_WEIGHT_OPEN;
        public static double BLAH_WEIGHT_COMMENT;
        public static double BLAH_WEIGHT_UPVOTES;
        public static double BLAH_WEIGHT_DOWNVOTES;
        public static double BLAH_WEIGHT_COMMENT_UPVOTES;
        public static double BLAH_WEIGHT_COMMENT_DOWNVOTES;

        public static double DELAY_BLAH_CHECK_THRESHOLD;
        public static int DELAY_BLAH_CHECK_HOURS;
        public static double BLAH_ACTIVE_THRESHOLD;

        public static double USER_WEIGHT_VIEW;
        public static double USER_WEIGHT_OPEN;
        public static double USER_WEIGHT_COMMENT;
        public static double USER_WEIGHT_UPVOTES;
        public static double USER_WEIGHT_DOWNVOTES;
        public static double USER_WEIGHT_COMMENT_UPVOTES;
        public static double USER_WEIGHT_COMMENT_DOWNVOTES;

        public static double DELAY_USER_CHECK_THRESHOLD;
        public static int DELAY_USER_CHECK_HOURS;
        public static double USER_ACTIVE_THRESHOLD;
    }

    public static class StrengthWorker {
        public static int QUEUE_VISIBLE_TIMEOUT_SECONDS;
        public static long NO_TASK_WAIT_MILLIS;

        public static int RECENT_BLAH_DAYS;

        public static double WEIGHT_VIEW;
        public static double WEIGHT_OPEN;
        public static double WEIGHT_COMMENT;
        public static double WEIGHT_UPVOTES;
        public static double WEIGHT_DOWNVOTES;
        public static double WEIGHT_COMMENT_UPVOTES;
        public static double WEIGHT_COMMENT_DOWNVOTES;
    }

    public static class InboxMonitor {
//        public static int START_HOUR;
        public static int PERIOD_HOURS;
    }

//    public static class InboxWorker {}

    public static class TransferInfoData {
        public static boolean USER_BLAH_INFO_TO_STATS;
    }

    public static class RandomNewActivity {
        public static boolean BLAH_NEW_ACTIVITY;
        public static boolean USER_NEW_ACTIVITY;
        public static long BLAH_WAIT_MILLIS;
        public static long USER_WAIT_MILLIS;
        public static double BLAH_NEW_ACT_PROBABILITY;
        public static double User_NEW_ACT_PROBABILITY;
    }
}
