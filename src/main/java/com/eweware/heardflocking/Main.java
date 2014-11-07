package com.eweware.heardflocking;

import com.eweware.heardflocking.base.HeardAzure;
import com.eweware.heardflocking.base.HeardDB;
import com.eweware.heardflocking.cohort.CohortMonitor;
import com.eweware.heardflocking.cohort.CohortWorker;
import com.eweware.heardflocking.inbox.InboxMonitor;
import com.eweware.heardflocking.strength.StrengthMonitor;
import com.eweware.heardflocking.strength.StrengthWorker;
import com.eweware.heardflocking.util.RandomNewActivity;
import com.eweware.heardflocking.util.TransferInfoData;

import java.io.*;
import java.util.Properties;

/**
 * Created by weihan on 11/1/14.
 */
public class Main {
    public static void main(String[] args) {
        new Main().execute(args);
    }

    private void execute(String[] args) {
        try {
            String service = parseArgument(args);
            if (service == null) return;

            getProperties();

            runService(service);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String mode = "dev";

    private void getProperties() throws IOException {
        System.out.println("load service properties");
        Properties prop = new Properties();
        String propFileName;

        if (mode.equals("dev")) propFileName = "./dev.properties";
        else if (mode.equals("qa")) propFileName = "./qa.properties";
        else propFileName = "./prod.properties";

        FileInputStream is = new FileInputStream(propFileName);

        if (is == null) {
            throw new FileNotFoundException("property file '" + propFileName + "' not found");
        }
        prop.load(is);

        getPropGlobal(prop);

        getPropTransferInfoDate(prop);
        getPropRandomNewActivity(prop);

        getPropCohortMonitor(prop);
        getPropCohortWorker(prop);
        getPropStrengthMonitor(prop);
        getPropStrengthWorker(prop);
        getPropInboxMonitor(prop);
    }

    private String parseArgument(String[] args) {
        if (args.length == 0 || args.length > 2) {
            System.out.println("Usage: java heard.jar service [dev|qa|prod]");
            System.out.println("service:");
            System.out.println("\tcw: CohortClusteringService");
            System.out.println("\tsm: StrengthMonitor");
            System.out.println("\tsw: StrengthTaskWorker");
            System.out.println("\tim: InboxMonitor");
            System.out.println("\ttd: TransferInfoData");
            System.out.println("\tra: RandomeNewActivity");
            System.out.println();
            return null;
        }
        else {
            if (args.length == 2) {
                if (args[1].equals("dev") || args[1].equals("qa") || args[1].equals("prod")) mode = args[1];
                else {
                    throw new IllegalArgumentException("Argument error : server can only be [dev|qa|prod] or not specified");
                }
                System.out.println("MongoDB mode=" + args[1]);
            }
            else {
                // defalut use dev server (localhost)
                System.out.println("MongoDB server=dev");
            }
            return args[0];
        }
    }

    private void runService(String service) throws Exception {
        HeardDB db = new HeardDB(mode);
        HeardAzure azure = new HeardAzure(mode);

        System.out.println("service=" + service);
        // choose process to run
        if (service.equals("cm")) {
            CohortMonitor.execute(db, azure);
        }
        else if (service.equals("cw")) {
            new CohortWorker(db, azure).execute();
        }
        else if (service.equals("sm")) {
            StrengthMonitor.execute(db, azure);
        }
        else if (service.equals("sw")) {
            new StrengthWorker(db, azure).execute();
        }
        else if (service.equals("im")) {
            InboxMonitor.execute(db, azure);
        }
        else if (service.equals("td")) {
            new TransferInfoData(db).execute();
        }
        else if (service.equals("ra")) {
            new RandomNewActivity(db).execute();
        }
        else {
            throw new IllegalArgumentException("Argument error : service can only be [cw|sm|sw|im|td|ra]");
        }
    }

    private void getPropGlobal(Properties prop) {
        ServiceProperties.TEST_ONLY_TECH = Boolean.parseBoolean(prop.getProperty("test_only_tech", "false"));
    }

    private void getPropTransferInfoDate(Properties prop) {
        ServiceProperties.TransferInfoData.USER_BLAH_INFO_TO_STATS = Boolean.parseBoolean(prop.getProperty("td.user_blah_info_to_stats", "false"));
    }

    private void getPropRandomNewActivity(Properties prop) {
        ServiceProperties.RandomNewActivity.BLAH_NEW_ACTIVITY = Boolean.parseBoolean(prop.getProperty("ra.blah_new_activity","false"));
        ServiceProperties.RandomNewActivity.USER_NEW_ACTIVITY = Boolean.parseBoolean(prop.getProperty("ra.user_new_activity","false"));
        ServiceProperties.RandomNewActivity.BLAH_WAIT_MILLIS = Long.parseLong(prop.getProperty("ra.blah_wait_millis", "1000"));
        ServiceProperties.RandomNewActivity.USER_WAIT_MILLIS = Long.parseLong(prop.getProperty("ra.user_wait_millis", "1000"));
        ServiceProperties.RandomNewActivity.BLAH_NEW_ACT_PROBABILITY = Double.parseDouble(prop.getProperty("ra.blah_new_act_probability", "0.01"));
        ServiceProperties.RandomNewActivity.User_NEW_ACT_PROBABILITY = Double.parseDouble(prop.getProperty("ra.user_new_act_probability", "0.01"));
    }

    private void getPropCohortMonitor(Properties prop) {
        ServiceProperties.CohortMonitor.START_HOUR = Integer.parseInt(prop.getProperty("cm.start_hour", "0"));
        ServiceProperties.CohortMonitor.PERIOD_HOURS = Integer.parseInt(prop.getProperty("cm.period_hours", "24"));
    }

    private void getPropCohortWorker(Properties prop) {
        ServiceProperties.CohortWorker.QUEUE_VISIBLE_TIMEOUT_SECONDS = Integer.parseInt(prop.getProperty("cw.queue_visible_timeout_seconds", "300"));
        ServiceProperties.CohortWorker.NO_TASK_WAIT_MILLIS = Integer.parseInt(prop.getProperty("cw.no_task_wait_millis", "10000"));
        ServiceProperties.CohortWorker.CLUSTERING_METHOD = prop.getProperty("cw.clustering_method", "trivial");
        ServiceProperties.CohortWorker.NUM_COHORTS = Integer.parseInt(prop.getProperty("cw.num_cohorts", "1"));
        ServiceProperties.CohortWorker.RECENT_BLAH_DAYS = Integer.parseInt(prop.getProperty("cw.recent_blah_days", "365"));
        ServiceProperties.CohortWorker.WEIGHT_VIEW = Double.parseDouble(prop.getProperty("cw.weight_view", "1.0"));
        ServiceProperties.CohortWorker.WEIGHT_OPEN = Double.parseDouble(prop.getProperty("cw.weight_open", "2.0"));
        ServiceProperties.CohortWorker.WEIGHT_COMMENT = Double.parseDouble(prop.getProperty("cw.weight_comment", "5.0"));
        ServiceProperties.CohortWorker.WEIGHT_UPVOTES = Double.parseDouble(prop.getProperty("cw.weight_upvotes", "10.0"));
        ServiceProperties.CohortWorker.WEIGHT_DOWNVOTES = Double.parseDouble(prop.getProperty("cw.weight_downvotes", "-10.0"));
        ServiceProperties.CohortWorker.WEIGHT_COMMENT_UPVOTES = Double.parseDouble(prop.getProperty("cw.weight_comment_upvotes", "5.0"));
        ServiceProperties.CohortWorker.WEIGHT_COMMENT_DOWNVOTES = Double.parseDouble(prop.getProperty("cw.weight_comment_downvotes", "-5.0"));
    }

    private void getPropStrengthMonitor(Properties prop) {
        ServiceProperties.StrengthMonitor.START_HOUR = Integer.parseInt(prop.getProperty("sm.start_hour", "0"));
        ServiceProperties.StrengthMonitor.PERIOD_MINUTES = Integer.parseInt(prop.getProperty("sm.period_minutes", "30"));
        ServiceProperties.StrengthMonitor.RECENT_BLAH_MONTHS = Integer.parseInt(prop.getProperty("sm.recent_blah_months", "24"));
    }

    private void getPropStrengthWorker(Properties prop) {
        ServiceProperties.StrengthWorker.QUEUE_VISIBLE_TIMEOUT_SECONDS = Integer.parseInt(prop.getProperty("sw.queue_visible_timeout_seconds","300"));
        ServiceProperties.StrengthWorker.NO_TASK_WAIT_MILLIS = Long.parseLong(prop.getProperty("sw.no_task_wait_millis","10000"));
        ServiceProperties.StrengthWorker.RECENT_BLAH_DAYS = Integer.parseInt(prop.getProperty("sw.recent_blah_months","12"));
        ServiceProperties.StrengthWorker.WEIGHT_VIEW = Double.parseDouble(prop.getProperty("sw.weight_view", "1.0"));
        ServiceProperties.StrengthWorker.WEIGHT_OPEN = Double.parseDouble(prop.getProperty("sw.weight_open", "2.0"));
        ServiceProperties.StrengthWorker.WEIGHT_COMMENT = Double.parseDouble(prop.getProperty("sw.weight_comment", "5.0"));
        ServiceProperties.StrengthWorker.WEIGHT_UPVOTES = Double.parseDouble(prop.getProperty("sw.weight_upvotes", "10.0"));
        ServiceProperties.StrengthWorker.WEIGHT_DOWNVOTES = Double.parseDouble(prop.getProperty("sw.weight_downvotes", "-10.0"));
        ServiceProperties.StrengthWorker.WEIGHT_COMMENT_UPVOTES = Double.parseDouble(prop.getProperty("sw.weight_comment_upvotes", "5.0"));
        ServiceProperties.StrengthWorker.WEIGHT_COMMENT_DOWNVOTES = Double.parseDouble(prop.getProperty("sw.weight_comment_downvotes", "-5.0"));
    }

    private void getPropInboxMonitor(Properties prop) {
//        ServiceProperties.InboxMonitor.START_HOUR = Integer.parseInt(prop.getProperty("im.start_hour", "0"));
        ServiceProperties.InboxMonitor.PERIOD_HOURS = Integer.parseInt(prop.getProperty("im.period_hours", "1"));
    }
}
