/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.trigger;

import azkaban.depFlows.*;
import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventHandler;
import azkaban.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.executor.Status;
import azkaban.mutFlows.MutFlows;
import azkaban.mutFlows.MutFlowsLoader;
import azkaban.mutFlows.jdbcMutFlowsLoader;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.trigger.builtin.KillExecutionAction;
import azkaban.utils.Props;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class TriggerManager extends EventHandler implements
    TriggerManagerAdapter {
  private static Logger logger = Logger.getLogger(TriggerManager.class);
  public static final long DEFAULT_SCANNER_INTERVAL_MS = 60000;

  private static Map<Integer, Trigger> triggerIdMap =
          new ConcurrentHashMap<Integer, Trigger>();

  private CheckerTypeLoader checkerTypeLoader;
  private ActionTypeLoader actionTypeLoader;
  private TriggerLoader triggerLoader;
  private DepFlowsLoader depFlowsLoader;
  private MutFlowsLoader mutFlowsLoader;
  private ExecutorManager executorManager;

  private final TriggerScannerThread runnerThread;
  private long lastRunnerThreadCheckTime = -1;
  private long runnerThreadIdleTime = -1;
  private LocalTriggerJMX jmxStats = new LocalTriggerJMX();

  private ExecutorManagerEventListener listener =
          new ExecutorManagerEventListener();

  private final Object syncObj = new Object();

  private String scannerStage = "";

  public TriggerManager(Props props, TriggerLoader triggerLoader,
                        ExecutorManager executorManager,DepFlowsLoader depFlowsLoader,jdbcMutFlowsLoader mutFlowsLoader) throws TriggerManagerException {
    this.executorManager = executorManager;
    this.triggerLoader = triggerLoader;
    this.depFlowsLoader = depFlowsLoader;
    this.mutFlowsLoader=mutFlowsLoader;
    long scannerInterval =
            props.getLong("trigger.scan.interval", DEFAULT_SCANNER_INTERVAL_MS);
    runnerThread = new TriggerScannerThread(scannerInterval);

    checkerTypeLoader = new CheckerTypeLoader();
    actionTypeLoader = new ActionTypeLoader();

    try {
      checkerTypeLoader.init(props);
      actionTypeLoader.init(props);
    } catch (Exception e) {
      throw new TriggerManagerException(e);
    }

    Condition.setCheckerLoader(checkerTypeLoader);
    Trigger.setActionTypeLoader(actionTypeLoader);

    executorManager.addListener(listener);

    logger.info("TriggerManager loaded.");
  }

  @Override
  public void start() throws TriggerManagerException {

    try {
      // expect loader to return valid triggers
      List<Trigger> triggers = triggerLoader.loadTriggers();
      for (Trigger t : triggers) {
        runnerThread.addTrigger(t);
        triggerIdMap.put(t.getTriggerId(), t);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new TriggerManagerException(e);
    }

    runnerThread.start();
  }

  protected CheckerTypeLoader getCheckerLoader() {
    return checkerTypeLoader;
  }

  protected ActionTypeLoader getActionLoader() {
    return actionTypeLoader;
  }

  public void insertTrigger(Trigger t) throws TriggerManagerException {
    synchronized (syncObj) {
      try {
        triggerLoader.addTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
      runnerThread.addTrigger(t);
      triggerIdMap.put(t.getTriggerId(), t);
    }
  }

  public void removeTrigger(int id) throws TriggerManagerException {
    synchronized (syncObj) {
      Trigger t = triggerIdMap.get(id);
      if (t != null) {
        removeTrigger(triggerIdMap.get(id));
      }
    }
  }

  public void updateTrigger(int id) throws TriggerManagerException {
    synchronized (syncObj) {
      if (!triggerIdMap.containsKey(id)) {
        throw new TriggerManagerException("The trigger to update " + id
                + " doesn't exist!");
      }

      Trigger t;
      try {
        t = triggerLoader.loadTrigger(id);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
      updateTrigger(t);
    }
  }

  public void updateTrigger(Trigger t) throws TriggerManagerException {
    synchronized (syncObj) {
      runnerThread.deleteTrigger(triggerIdMap.get(t.getTriggerId()));
      runnerThread.addTrigger(t);
      triggerIdMap.put(t.getTriggerId(), t);
    }
  }

  public void removeTrigger(Trigger t) throws TriggerManagerException {
    synchronized (syncObj) {
      runnerThread.deleteTrigger(t);
      triggerIdMap.remove(t.getTriggerId());
      try {
        t.stopCheckers();
        triggerLoader.removeTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
    }
  }

  public List<Trigger> getTriggers() {
    return new ArrayList<Trigger>(triggerIdMap.values());
  }

  public Map<String, Class<? extends ConditionChecker>> getSupportedCheckers() {
    return checkerTypeLoader.getSupportedCheckers();
  }

  private class TriggerScannerThread extends Thread {
    private BlockingQueue<Trigger> triggers;
    private Map<Integer, ExecutableFlow> justFinishedFlows;
    private boolean shutdown = false;
    private final long scannerInterval;

    public TriggerScannerThread(long scannerInterval) {
      triggers = new PriorityBlockingQueue<Trigger>(1, new TriggerComparator());
      justFinishedFlows = new ConcurrentHashMap<Integer, ExecutableFlow>();
      this.setName("TriggerRunnerManager-Trigger-Scanner-Thread");
      this.scannerInterval = scannerInterval;
    }

    public void shutdown() {
      logger.error("Shutting down trigger manager thread " + this.getName());
      shutdown = true;
      this.interrupt();
    }

    public void addJustFinishedFlow(ExecutableFlow flow) {
      synchronized (syncObj) {
        justFinishedFlows.put(flow.getExecutionId(), flow);
      }
    }

    public void addTrigger(Trigger t) {
      synchronized (syncObj) {
        t.updateNextCheckTime();
        triggers.add(t);
      }
    }

    public void deleteTrigger(Trigger t) {
      triggers.remove(t);
    }

    public void run() {
      while (!shutdown) {
        synchronized (syncObj) {
          try {
            lastRunnerThreadCheckTime = System.currentTimeMillis();

            scannerStage =
                    "Ready to start a new scan cycle at "
                            + lastRunnerThreadCheckTime;

            try {
              checkAllTriggers();
              justFinishedFlows.clear();
            } catch (Exception e) {
              e.printStackTrace();
              logger.error(e.getMessage());
            } catch (Throwable t) {
              t.printStackTrace();
              logger.error(t.getMessage());
            }

            scannerStage = "Done flipping all triggers.";

            runnerThreadIdleTime =
                    scannerInterval
                            - (System.currentTimeMillis() - lastRunnerThreadCheckTime);

            if (runnerThreadIdleTime < 0) {
              logger.error("Trigger manager thread " + this.getName()
                      + " is too busy!");
            } else {
              syncObj.wait(runnerThreadIdleTime);
            }
          } catch (InterruptedException e) {
            logger.info("Interrupted. Probably to shut down.");
          }
        }
      }
    }

    private void checkAllTriggers() throws TriggerManagerException {
      long now = System.currentTimeMillis();

      // sweep through the rest of them
      for (Trigger t : triggers) {
        try {
          scannerStage = "Checking for trigger " + t.getTriggerId();
          boolean shouldSkip = true;
          if (shouldSkip && t.getInfo() != null && t.getInfo().containsKey("monitored.finished.execution")) {
            int execId = Integer.valueOf((String) t.getInfo().get("monitored.finished.execution"));
            if (justFinishedFlows.containsKey(execId)) {
              logger.info("Monitored execution has finished. Checking trigger earlier " + t.getTriggerId());
              shouldSkip = false;
            }
          }
          if (shouldSkip && t.getNextCheckTime() > now) {
            shouldSkip = false;
          }

          logger.info("Get Next Check Time =" + t.getNextCheckTime() + "  now = " + now);
          if (shouldSkip) {
            logger.info("Skipping trigger" + t.getTriggerId() + " until " + t.getNextCheckTime());
          }

          if (logger.isDebugEnabled()) {
            logger.info("Checking trigger " + t.getTriggerId());
          }
          logger.info("trigger的状态"+t.getStatus());
          logger.info(t.triggerConditionMet());
          if (t.getStatus().equals(TriggerStatus.READY)) {
            logger.info("进入triggers检查");
            if (t.triggerConditionMet()) {
              logger.info("进入triggers检查");
              /**********************/
              List<TriggerAction> actions = t.getActions();
              logger.info("获取triggeraction:"+actions);
              //actions.get(0).getDescription().split("flowName")[1].split("\"")[2]
              logger.info("判断是否可以转为ExecuteFlowAction");
              if(actions.get(0) instanceof ExecuteFlowAction) {
                logger.info("进入强转");
                ExecuteFlowAction a = (ExecuteFlowAction) actions.get(0);
                logger.info("获取的ExecuteFlowAction:"+a);
                   String flowName=a.getFlowName();
                  int projectId = a.getProjectId();
                logger.info("打印参数 "+flowName+"----------"+projectId);
                  List<DepFlows> flows = depFlowsLoader.loadDepFlows(flowName,projectId);
                logger.info("获取flow的依赖 "+flows.size());
                List<MutFlows> mflows = mutFlowsLoader.loadMutFlows(flowName,projectId);
                logger.info("获取flow的互斥 :"+mflows.size());
                if(flows.size()!=0||mflows.size()!=0)
                {
                  boolean dflag=true;
                  boolean mflag=true;
                  ExecutableFlow depf = new ExecutableFlow();
                  ExecutableFlow mutf = new ExecutableFlow();
                  if(flows.size()!=0) {
                    dflag=false;
                    /*********************************/
                    List<ExecutableFlow> e1 = new ArrayList<ExecutableFlow>();
                    e1=executorManager.getExecutableFlows(flows.get(0).getDepProjectId(), flows.get(0).getDepFlowId(), 0, 20, Status.READY);
                    List<ExecutableFlow> e2 =new ArrayList<ExecutableFlow>();
                    e2=executorManager.getExecutableFlows(flows.get(0).getDepProjectId(), flows.get(0).getDepFlowId(), 0, 20, Status.PREPARING);
                    List<ExecutableFlow> e3 = new ArrayList<ExecutableFlow>();
                    e3=executorManager.getExecutableFlows(flows.get(0).getDepProjectId(), flows.get(0).getDepFlowId(), 0, 20, Status.PAUSED);
                    List<ExecutableFlow> e4 =new ArrayList<ExecutableFlow>();
                    e4=executorManager.getExecutableFlows(flows.get(0).getDepProjectId(), flows.get(0).getDepFlowId(), 0, 20, Status.RUNNING);
                    /*********************************/
                    if(e1.size()>0||e2.size()>0||e3.size()>0||e4.size()>0)
                    {dflag=false;}
                    else {
                      List<ExecutableFlow> exeflows = executorManager.getExecutableFlows(flows.get(0).getDepProjectId(), flows.get(0).getDepFlowId(), 0, 20, Status.SUCCEEDED);
                      logger.info("获取flow的依赖的流列表和状态" + exeflows.size());
                      if (exeflows.size()>0) {
                        logger.info("获取flow的依赖的流数量" + exeflows.size());
                        for(ExecutableFlow flow:exeflows)
                        {
                          logger.info(flow.getEndTime());
                          logger.info(DateUtils.isToday(flow.getEndTime()));
                        }
                        if (DateUtils.isToday(exeflows.get(0).getEndTime())) {
                          logger.info("有今天的依赖流完成" + exeflows.get(0).getId());
                          dflag = true;
                        }
                      }
                    }
                  }
                  if(mflows.size()!=0) {
                    logger.info("有互斥流");
                    logger.info(mflows.get(0).toString());
                    List<ExecutableFlow> m4 =new ArrayList<ExecutableFlow>();
                    List<ExecutableFlow> m1 = new ArrayList<ExecutableFlow>();
                    m1=executorManager.getExecutableFlows(mflows.get(0).getMutProjectId(), mflows.get(0).getMutFlowId(), 0, 20, Status.READY);
                    List<ExecutableFlow> m2 =new ArrayList<ExecutableFlow>();
                    m2=executorManager.getExecutableFlows(mflows.get(0).getMutProjectId(), mflows.get(0).getMutFlowId(), 0, 20, Status.PREPARING);
                    List<ExecutableFlow> m3 = new ArrayList<ExecutableFlow>();
                    m3=executorManager.getExecutableFlows(mflows.get(0).getMutProjectId(), mflows.get(0).getMutFlowId(), 0, 20, Status.PAUSED);
                    List<ExecutableFlow> mexeflows =new ArrayList<ExecutableFlow>();
                    mexeflows=executorManager.getExecutableFlows(mflows.get(0).getMutProjectId(), mflows.get(0).getMutFlowId(), 0, 20, Status.RUNNING);
                    for (ExecutableFlow flow:mexeflows)
                    {
                      m4.add(flow);
                    }
                    for (ExecutableFlow flow:m1)
                   {
                     m4.add(flow);
                   }
                    for (ExecutableFlow flow:m2)
                    {
                      m4.add(flow);
                    }
                    for (ExecutableFlow flow:m3)
                    {
                      m4.add(flow);
                    }
                    logger.info("获取flow的互斥的流列表和状态" + m4.size());
                    for (ExecutableFlow flow : m4) {
                      logger.info("遍历互斥流详情" + flow.getFlowId());
                      logger.info(flow.getStartTime());
                      logger.info(DateUtils.isToday(flow.getStartTime()));
                    }
                    if(m4.size()>0) {
                      if (DateUtils.isToday(m4.get(0).getSubmitTime())) {
                        logger.info("有今天的互斥流在运行" + m4.get(0).getId());
                        mflag = false;
                        mutf = m4.get(0);
                      }
                    }
                  }
                  logger.info("是否存在符合依赖条件的流 -----"+dflag);
                  logger.info("是否存在符合互斥条件的流 ----"+mflag);
                  if (dflag&&mflag) {
                    logger.info("继续检查triggle");
                    onTriggerTrigger(t);

                  }else
                  {
                    logger.info("让流执行时间延迟");
                    t.resetTriggerConditionsWithSleep();
                    logger.info("下次执行时间："+t.getNextCheckTime());
                  }
                }else
                {
                  logger.info("没有依赖或互斥，直接执行");
                  onTriggerTrigger(t);
                }
              /**********************/
              }
            } else if (t.expireConditionMet()) {
              onTriggerExpire(t);
            }
          }
          if (t.getStatus().equals(TriggerStatus.EXPIRED) && t.getSource().equals("azkaban")) {
            removeTrigger(t);
          } else {
            t.updateNextCheckTime();
          }
        } catch (Throwable th) {
          //skip this trigger, moving on to the next one
          logger.error("Failed to process trigger with id : " + t.getTriggerId(), th);
        }
      }
    }

    private void onTriggerTrigger(Trigger t) throws TriggerManagerException {
      List<TriggerAction> actions = t.getTriggerActions();
      for (TriggerAction action : actions) {
        try {
          logger.info("Doing trigger actions");
          action.doAction();
        } catch (Exception e) {
          logger.error("Failed to do action " + action.getDescription(), e);
        } catch (Throwable th) {
          logger.error("Failed to do action " + action.getDescription(), th);
        }
      }
      if (t.isResetOnTrigger()) {
        t.resetTriggerConditions();
        t.resetExpireCondition();
      } else {
        t.setStatus(TriggerStatus.EXPIRED);
      }
      try {
        triggerLoader.updateTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
    }

    private void onTriggerExpire(Trigger t) throws TriggerManagerException {
      List<TriggerAction> expireActions = t.getExpireActions();
      for (TriggerAction action : expireActions) {
        try {
          logger.info("Doing expire actions");
          action.doAction();
        } catch (Exception e) {
          logger.error("Failed to do expire action " + action.getDescription(),
                  e);
        } catch (Throwable th) {
          logger.error("Failed to do expire action " + action.getDescription(),
                  th);
        }
      }
      if (t.isResetOnExpire()) {
        t.resetTriggerConditions();
        t.resetExpireCondition();
      } else {
        t.setStatus(TriggerStatus.EXPIRED);
      }
      try {
        triggerLoader.updateTrigger(t);
      } catch (TriggerLoaderException e) {
        throw new TriggerManagerException(e);
      }
    }

    private class TriggerComparator implements Comparator<Trigger> {
      @Override
      public int compare(Trigger arg0, Trigger arg1) {
        long first = arg1.getNextCheckTime();
        long second = arg0.getNextCheckTime();

        if (first == second) {
          return 0;
        } else if (first < second) {
          return 1;
        }
        return -1;
      }
    }
  }

  public Trigger getTrigger(int triggerId) {
    synchronized (syncObj) {
      return triggerIdMap.get(triggerId);
    }
  }

  public void expireTrigger(int triggerId) {
    Trigger t = getTrigger(triggerId);
    t.setStatus(TriggerStatus.EXPIRED);
  }

  @Override
  public List<Trigger> getTriggers(String triggerSource) {
    List<Trigger> triggers = new ArrayList<Trigger>();
    for (Trigger t : triggerIdMap.values()) {
      if (t.getSource().equals(triggerSource)) {
        triggers.add(t);
      }
    }
    return triggers;
  }

  @Override
  public List<Trigger> getTriggerUpdates(String triggerSource,
                                         long lastUpdateTime) throws TriggerManagerException {
    List<Trigger> triggers = new ArrayList<Trigger>();
    for (Trigger t : triggerIdMap.values()) {
      if (t.getSource().equals(triggerSource)
              && t.getLastModifyTime() > lastUpdateTime) {
        triggers.add(t);
      }
    }
    return triggers;
  }

  @Override
  public List<Trigger> getAllTriggerUpdates(long lastUpdateTime)
          throws TriggerManagerException {
    List<Trigger> triggers = new ArrayList<Trigger>();
    for (Trigger t : triggerIdMap.values()) {
      if (t.getLastModifyTime() > lastUpdateTime) {
        triggers.add(t);
      }
    }
    return triggers;
  }

  @Override
  public void insertTrigger(Trigger t, String user)
          throws TriggerManagerException {
    insertTrigger(t);
  }

  @Override
  public void removeTrigger(int id, String user) throws TriggerManagerException {
    removeTrigger(id);
  }

  @Override
  public void updateTrigger(Trigger t, String user)
          throws TriggerManagerException {
    updateTrigger(t);
  }

  @Override
  public void shutdown() {
    runnerThread.shutdown();
  }

  @Override
  public TriggerJMX getJMX() {
    return this.jmxStats;
  }

  private class LocalTriggerJMX implements TriggerJMX {

    @Override
    public long getLastRunnerThreadCheckTime() {
      return lastRunnerThreadCheckTime;
    }

    @Override
    public boolean isRunnerThreadActive() {
      return runnerThread.isAlive();
    }

    @Override
    public String getPrimaryServerHost() {
      return "local";
    }

    @Override
    public int getNumTriggers() {
      return triggerIdMap.size();
    }

    @Override
    public String getTriggerSources() {
      Set<String> sources = new HashSet<String>();
      for (Trigger t : triggerIdMap.values()) {
        sources.add(t.getSource());
      }
      return sources.toString();
    }

    @Override
    public String getTriggerIds() {
      return triggerIdMap.keySet().toString();
    }

    @Override
    public long getScannerIdleTime() {
      return runnerThreadIdleTime;
    }

    @Override
    public Map<String, Object> getAllJMXMbeans() {
      return new HashMap<String, Object>();
    }

    @Override
    public String getScannerThreadStage() {
      return scannerStage;
    }

  }

  @Override
  public void registerCheckerType(String name,
                                  Class<? extends ConditionChecker> checker) {
    checkerTypeLoader.registerCheckerType(name, checker);
  }

  @Override
  public void registerActionType(String name,
                                 Class<? extends TriggerAction> action) {
    actionTypeLoader.registerActionType(name, action);
  }

  private class ExecutorManagerEventListener implements EventListener {
    public ExecutorManagerEventListener() {
    }

    @Override
    public void handleEvent(Event event) {
      // this needs to be fixed for perf
      synchronized (syncObj) {
        ExecutableFlow flow = (ExecutableFlow) event.getRunner();
        if (event.getType() == Type.FLOW_FINISHED) {
          logger.info("Flow finish event received. " + flow.getExecutionId());
          runnerThread.addJustFinishedFlow(flow);
        }
      }
    }
  }


/*  public static void main(String[] args) {
    Props props = new Props();
    props.put("mysql.port", 3306);
    props.put("database.type","mysql");
    props.put("mysql.host","172.20.0.28");
    props.put("mysql.database","azkaban");
    props.put("mysql.user","root");
    props.put("mysql.password","handhand");
    props.put("mysql.numconnections", 10);
    System.out.println(new jdbcDepFlowsLoader(props).loadDepFlows("test2",0));
    try {
      TriggerLoader loader = new JdbcTriggerLoader(props);
      jdbcDepFlowsLoader flowsLoader=new jdbcDepFlowsLoader(props);
      JdbcExecutorLoader loader1 = new JdbcExecutorLoader(props);
      ExecutorManager execManager = new ExecutorManager(props, loader1, null);
     // new TriggerManager(props, loader, execManager,flowsLoader);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }*/
}
