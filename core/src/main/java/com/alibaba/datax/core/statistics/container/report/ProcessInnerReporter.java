package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerReporter extends AbstractReporter {

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        // do nothing
    }

    @Override
    public void reportTGCommunication(Long jobId, Integer taskGroupId, Communication communication) {
        LocalTGCommunicationManager.updateTaskGroupCommunication(jobId, taskGroupId, communication);
    }
}