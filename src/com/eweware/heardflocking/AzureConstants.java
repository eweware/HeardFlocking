package com.eweware.heardflocking;

/**
 * Created by weihan on 10/22/14.
 */
public interface AzureConstants {
    public static final String STORAGE_CONNECTION_STRING =
            "DefaultEndpointsProtocol=http;" +
                    "AccountName=weihanstorage;" +
                    "AccountKey=PKz1eXkKlu07u4SpfyxfCvO1BH4yZCnuXhrQbebIaOdmUfGGD6qV8r+lycj7sNXSwtVTHpo/nJBlHVa4oavNgg==";

    public static final String STRENGTH_TASK_QUEUE = "strengthtaskqueue";

    public interface StrengthTask {
        public static final String TYPE = "T";
        public static final String BLAH_ID = "B";
        public static final String USER_ID = "U";
        public static final String GROUP_ID = "G";

        public static final int COMPUTE_BLAH_STRENGTH = 0;
        public static final int COMPUTE_USER_STRENGTH = 1;
    }
}
