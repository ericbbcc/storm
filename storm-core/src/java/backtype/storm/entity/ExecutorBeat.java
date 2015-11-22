package backtype.storm.entity;

import backtype.storm.generated.ClusterWorkerHeartbeat;
import backtype.storm.generated.ExecutorStats;

/**
 * @author float.lu
 */
public class ExecutorBeat {
    private ClusterWorkerHeartbeat workerHeartbeat;
    private ExecutorStats executorStats;

    public ClusterWorkerHeartbeat getWorkerHeartbeat() {
        return workerHeartbeat;
    }

    public void setWorkerHeartbeat(ClusterWorkerHeartbeat workerHeartbeat) {
        this.workerHeartbeat = workerHeartbeat;
    }

    public ExecutorStats getExecutorStats() {
        return executorStats;
    }

    public void setExecutorStats(ExecutorStats executorStats) {
        this.executorStats = executorStats;
    }
}
