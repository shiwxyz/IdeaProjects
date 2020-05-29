package com.shiwxyz.bigdata;

public class StudentCommitModes {


    public static StudentCommitMode fromConf(
            boolean isOverTime,
            boolean isOnTime,
            boolean isInAdvance
    ) {
        if (isOnTime) {
            return (isOnTime) ? StudentCommitMode.ON_TIME : StudentCommitMode.OVERTIME;
        } else {
            return (isInAdvance) ? StudentCommitMode.IN_ADVANCE : StudentCommitMode.OVERTIME;
        }
    }
}
