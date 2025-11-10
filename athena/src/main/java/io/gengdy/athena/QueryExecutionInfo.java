package io.gengdy.athena;

import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryExecutionInfo
{
    int queryID;
    String traceToken;
    long submissionTime;
    double pendingTime;
    double executionTime;
    double costCents;
    boolean isFinished;
    String status;

    public QueryExecutionInfo(int queryID, String tranceToken,
                              long submissionTime, boolean isFinished)
    {
        this.queryID = queryID;
        this.traceToken = tranceToken;
        this.submissionTime = submissionTime;
        this.isFinished = isFinished;
        this.status = "SUBMITTED";
    }

    public String getTraceToken()
    {
        return traceToken;
    }

    public boolean isFinished()
    {
        return isFinished;
    }

    public void setFinished(String finalStatus)
    {
        this.isFinished = true;
        this.status = finalStatus;
    }

    public void setPendingTime(double pendingTime)
    {
        this.pendingTime = pendingTime;
    }

    public void setExecutionTime(double executionTime)
    {
        this.executionTime = executionTime;
    }

    public void setCostCents(double costCents)
    {
        this.costCents = costCents;
    }

    @Override
    public String toString()
    {
        String queryIdentifier = "Q" + queryID;
        String submissionTimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date(submissionTime));

        if (status.equals("FAILED")) {
            return "QueryExecutionInfo{" +
                    "queryID=" + queryIdentifier +
                    ", submissionTime=" + submissionTimeStr +
                    ", status=FAILED" +
                    ", pendingTime= N/A" +
                    ", executionTime= N/A" +
                    ", costCents= N/A" +
                    '}';
        }

        return "QueryExecutionInfo{" +
                "queryID=" + queryIdentifier +
                ", submissionTime=" + submissionTimeStr +
                ", status=" + status +
                ", pendingTime=" + pendingTime + " ms" +
                ", executionTime=" + executionTime + " ms" +
                ", costCents=" + String.format("%.10f", costCents) +
                '}';
    }
}
