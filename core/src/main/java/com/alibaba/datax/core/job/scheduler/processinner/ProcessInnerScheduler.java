package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ProcessInnerScheduler extends AbstractScheduler {

    private ExecutorService taskGroupContainerExecutorService;
    private List<TaskGroupContainerRunner> runners;

    public ProcessInnerScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public void startAllTaskGroup(List<Configuration> configurations) {
        this.taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());
        this.runners = new ArrayList<>();
        for (Configuration taskGroupConfiguration : configurations) {
            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
            this.runners.add(taskGroupContainerRunner);
            this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
        }

        this.taskGroupContainerExecutorService.shutdown();
    }

    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }


    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                "job killed status");
    }

    @Override
    public void dealCanceledStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        this.taskGroupContainerExecutorService.shutdownNow();
    }


    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }

}
