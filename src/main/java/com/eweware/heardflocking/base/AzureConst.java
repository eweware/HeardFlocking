package com.eweware.heardflocking.base;

/**
 * Created by weihan on 10/22/14.
 */
public interface AzureConst {
    static final String DEV_ACCOUNT_NAME = "weihanstorage";
    static final String DEV_ACCOUNT_KEY = "PKz1eXkKlu07u4SpfyxfCvO1BH4yZCnuXhrQbebIaOdmUfGGD6qV8r+lycj7sNXSwtVTHpo/nJBlHVa4oavNgg==";

    public static final String QA_ACCOUNT_NAME = "heardqueueqa";
    public static final String QA_ACCOUNT_KEY = "dr3XhxQEKlwqSGPe9+YJiwCUZ2v7izLOR31xED66joJcyUWJoDU9A1Hl0HzlXa/WsLorEYEpscNU06p0TYGcjA==";

    public static final String PROD_ACCOUNT_NAME = "";
    public static final String PROD_ACCOUNT_KEY = "";

    public static final String COHORT_TASK_QUEUE = "cohorttasks";
    public static final String STRENGTH_TASK_QUEUE = "strengthtasks";
    public static final String INBOX_TASK_QUEUE = "inboxtasks";

    public interface CohortTask {
        public static final String TYPE = "T";
        public static final String GROUP_ID = "G";

        public static final int RECLUSTER = 0;
    }

    public interface StrengthTask {
        public static final String TYPE = "T";
        public static final String BLAH_ID = "B";
        public static final String USER_ID = "U";
        public static final String GROUP_ID = "G";
        public static final String GENERATION_ID = "GEN";

        public static final int COMPUTE_BLAH_STRENGTH = 0;
        public static final int COMPUTE_USER_STRENGTH = 1;
        public static final int COMPUTE_ALL_STRENGTH = 2;
    }

    public interface InboxTask {
        public static final String TYPE = "T";
        public static final String GROUP_ID = "G";
        public static final String GENERATION_ID = "GEN";

        public static final int GENERATE_INBOX = 0;
        public static final int GENERATE_INBOX_NEW_CLUSTER = 1;
    }
}
