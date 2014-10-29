package com.eweware.heardflocking;

/**
 * Created by weihan on 10/22/14.
 */
public interface AzureConstants {
    public static final String AZURE_ACCOUNT_NAME = "weihanstorage";
    public static final String AZURE_ACCOUNT_KEY = "AccountKey=PKz1eXkKlu07u4SpfyxfCvO1BH4yZCnuXhrQbebIaOdmUfGGD6qV8r+lycj7sNXSwtVTHpo/nJBlHVa4oavNgg==";

    public static final String STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=" + AZURE_ACCOUNT_NAME + ";" + AZURE_ACCOUNT_KEY;

    public static final String STRENGTH_TASK_QUEUE = "strengthtaskqueue";
    public static final String INBOX_TASK_QUEUE = "inboxtaskqueue";

    public interface StrengthTask {
        public static final String TYPE = "T";
        public static final String BLAH_ID = "B";
        public static final String USER_ID = "U";
        public static final String GROUP_ID = "G";

        public static final int COMPUTE_BLAH_STRENGTH = 0;
        public static final int COMPUTE_USER_STRENGTH = 1;
        public static final int COMPUTE_ALL_BLAH_STRENGTH = 2;
        public static final int COMPUTE_ALL_USER_STRENGTH = 3;
    }

    public interface InboxTask {
        public static final String TYPE = "T";
        public static final String GROUP_ID = "G";

        public static final int GENERATE_INBOX = 0;
        public static final int GENERATE_INBOX_NEW_CLUSTER = 1;
    }
}
