package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalTGCommunicationManager {
    private static Map<Long, Map<Integer, Communication>> jobId2TaskGroupCommunicationMap =
            new ConcurrentHashMap<>();

    public static void registerTaskGroupCommunication(
            long jobId, int taskGroupId, Communication communication) {
        Map<Integer, Communication> taskGroupCommunicationMap =
                jobId2TaskGroupCommunicationMap.computeIfAbsent(jobId, key -> new HashMap<>());
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    public static Communication getJobCommunication(long jobId) {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        Map<Integer, Communication> taskGroupCommunicationMap =
                jobId2TaskGroupCommunicationMap.getOrDefault(jobId, new HashMap<>());
        for (Communication taskGroupCommunication :
                taskGroupCommunicationMap.values()) {
            communication.mergeFrom(taskGroupCommunication);
        }

        return communication;
    }

    /**
     * 采用获取taskGroupId后再获取对应communication的方式，
     * 防止map遍历时修改，同时也防止对map key-value对的修改
     *
     * @return
     */
    public static Set<Integer> getTaskGroupIdSet(long jobId) {
        return jobId2TaskGroupCommunicationMap.getOrDefault(jobId, new HashMap<>()).keySet();
    }

    public static Communication getTaskGroupCommunication(long jobId, int taskGroupId) {
        Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");

        return jobId2TaskGroupCommunicationMap.getOrDefault(jobId, new HashMap<>()).get(taskGroupId);
    }

    public static void updateTaskGroupCommunication(final long jobId, final int taskGroupId,
                                                    final Communication communication) {
        Map<Integer, Communication> taskGroupCommunicationMap =
                jobId2TaskGroupCommunicationMap.getOrDefault(jobId, new HashMap<>());

        Validate.isTrue(taskGroupCommunicationMap.containsKey(
                taskGroupId), String.format("taskGroupCommunicationMap中没有注册taskGroupId[%d]的Communication，" +
                "无法更新该taskGroup的信息", taskGroupId));
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    public static void clear(long jobId) {
        jobId2TaskGroupCommunicationMap.getOrDefault(jobId, new HashMap<>()).clear();
    }

    public static Map<Integer, Communication> getTaskGroupCommunicationMap(long jobId) {
        return jobId2TaskGroupCommunicationMap.getOrDefault(jobId, new HashMap<>());
    }
}