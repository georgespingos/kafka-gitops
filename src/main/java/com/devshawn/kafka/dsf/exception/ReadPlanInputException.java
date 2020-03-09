package com.devshawn.kafka.dsf.exception;

public class ReadPlanInputException extends RuntimeException {

    public ReadPlanInputException(String exMessage) {
        super(String.format("Error reading execution plan from file: %s", exMessage));
    }
}