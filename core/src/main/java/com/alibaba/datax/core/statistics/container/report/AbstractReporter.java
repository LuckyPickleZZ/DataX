package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;

public abstract class AbstractReporter {

    public abstract void reportJobCommunication(Long jobId, Communication communication);

    public abstract void reportTGCommunication(Long jobId, Integer taskGroupId, Communication communication);

}
