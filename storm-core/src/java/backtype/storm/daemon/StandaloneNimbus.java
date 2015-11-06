package backtype.storm.daemon;

import backtype.storm.scheduler.*;

import java.util.*;

/**
 * 基本的Nimbus实现
 * @author float.lu
 */
public class StandaloneNimbus implements INimbus {

    @Override
    public void prepare(Map stormConf, String schedulerLocalDir) {
        //TODO nothing
    }

    @Override
    public Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
        if(existingSupervisors == null || existingSupervisors.size() == 0){
            return Collections.EMPTY_LIST;
        }
        Collection<WorkerSlot> workerSlots = new ArrayList<>();
        for(SupervisorDetails supervisorDetails : existingSupervisors){
            workerSlots.addAll(makeWorkSlotsFromSupervisorDetails(supervisorDetails));
        }
        return workerSlots;
    }

    @Override
    public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {
        //TODO nothing
    }

    @Override
    public String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
        SupervisorDetails supervisorDetails = existingSupervisors.get(nodeId);
        if(supervisorDetails != null){
            return supervisorDetails.getHost();
        }
        return null;
    }

    @Override
    public IScheduler getForcedScheduler() {
        return null;
    }

    private Collection makeWorkSlotsFromSupervisorDetails(SupervisorDetails s){
        Collection<Number> ports = (Collection)s.getMeta();
        if(ports == null || ports.size() == 0){
            return Collections.EMPTY_LIST;
        }
        Collection<WorkerSlot> workerSlots = new ArrayList<>();
        for(Number port : ports){
            WorkerSlot workerSlot = new WorkerSlot(s.getId(), port);
            workerSlots.add(workerSlot);
        }
        return workerSlots;
    }
}
