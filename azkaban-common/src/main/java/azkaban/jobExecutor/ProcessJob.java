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

package azkaban.jobExecutor;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import azkaban.executionParams.DispExecutionParams;
import azkaban.executionParams.JdbcDispExecutionParamsLoader;
import azkaban.utils.JSONUtils;
import azkaban.wfJobParams.DispWfJobParams;
import azkaban.wfJobParams.JdbcDispWfJobParamsLoader;
import com.google.common.io.Files;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.SystemMemoryInfo;

import static azkaban.flow.CommonJobProperties.*;
import static azkaban.flow.SpecialJobTypes.FLOW_NAME;

/**
 * A job that runs a simple unix command
 */
public class ProcessJob extends AbstractProcessJob {

    public static final String COMMAND = "command";

    private static final long KILL_TIME_MS = 5000;

    private volatile AzkabanProcess process;

    private static final String MEMCHECK_ENABLED = "memCheck.enabled";

    private static final String MEMCHECK_FREEMEMDECRAMT =
            "memCheck.freeMemDecrAmt";

    public static final String AZKABAN_MEMORY_CHECK = "azkaban.memory.check";

    public static final String NATIVE_LIB_FOLDER = "azkaban.native.lib";
    public static final String EXECUTE_AS_USER = "execute.as.user";
    public static final String EXECUTE_AS_USER_OVERRIDE =
            "execute.as.user.override";
    public static final String USER_TO_PROXY = "user.to.proxy";
    public static final String KRB5CCNAME = "KRB5CCNAME";
    private JdbcDispExecutionParamsLoader dispExecutionParamsLoader;
    private JdbcDispWfJobParamsLoader dispWfJobParamsLoader;

    public ProcessJob(final String jobId, final Props sysProps,
                      final Props jobProps, final Logger log) {
        super(jobId, sysProps, jobProps, log);

        // this is in line with what other job types (hadoopJava, spark, pig, hive)
        // is doing
        jobProps.put(CommonJobProperties.JOB_ID, jobId);
    }

    @Override
    public void run() throws Exception {
        dispExecutionParamsLoader = dispExecutionParamsLoader!=null?dispExecutionParamsLoader:super.getDispExecutionParamsLoader();
        dispWfJobParamsLoader = dispWfJobParamsLoader!=null?dispWfJobParamsLoader:super.getDispWfJobParamsLoader();
        info(String.format("ProcessJob中的extra:%s", jobProps.getString("extra", "{}")));
        try {
            resolveProps();
        } catch (Exception e) {
            handleError("Bad property definition! " + e.getMessage(), e);
        }

        if (sysProps.getBoolean(MEMCHECK_ENABLED, true)
                && jobProps.getBoolean(AZKABAN_MEMORY_CHECK, true)) {
            long freeMemDecrAmt = sysProps.getLong(MEMCHECK_FREEMEMDECRAMT, 0);
            Pair<Long, Long> memPair = getProcMemoryRequirement();
            boolean isMemGranted =
                    SystemMemoryInfo.canSystemGrantMemory(memPair.getFirst(),
                            memPair.getSecond(), freeMemDecrAmt);
            if (!isMemGranted) {
                throw new Exception(
                        String
                                .format(
                                        "Cannot request memory (Xms %d kb, Xmx %d kb) from system for job %s",
                                        memPair.getFirst(), memPair.getSecond(), getId()));
            }
        }

        List<String> commands = null;
        try {
            commands = getCommandList();
        } catch (Exception e) {
            handleError("Job set up failed " + e.getCause(), e);
        }

        long startMs = System.currentTimeMillis();

        if (commands == null) {
            handleError("There are no commands to execute", null);
        }

        info(commands.size() + " commands to execute.");
        File[] propFiles = initPropsFiles();

        // change krb5ccname env var so that each job execution gets its own cache
        Map<String, String> envVars = getEnvironmentVariables();
        envVars.put(KRB5CCNAME, getKrb5ccname(jobProps));

        // determine whether to run as Azkaban or run as effectiveUser
        String executeAsUserBinaryPath = null;
        String effectiveUser = null;
        boolean isExecuteAsUser = determineExecuteAsUser(sysProps, jobProps);

        if (isExecuteAsUser) {
            String nativeLibFolder = sysProps.getString(NATIVE_LIB_FOLDER);
            executeAsUserBinaryPath =
                    String.format("%s/%s", nativeLibFolder, "execute-as-user");
            effectiveUser = getEffectiveUser(jobProps);
            if ("root".equals(effectiveUser)) {
                throw new RuntimeException(
                        "Not permitted to proxy as root through Azkaban");
            }
        }
        info("-------------jobPros" + jobProps.toString());
        File tmpDir = Files.createTempDir();
        String workflowCode = jobProps.getString(PROJECT_NAME);
        String jobCode = jobProps.getString(CommonJobProperties.JOB_ID);
        Long execId = jobProps.getLong(EXEC_ID);
        /**
         * 如果是子任务流就获取子任务流的参数，否则就获取当前任务流上的参数
         */
        if (jobProps.containsKey(FLOW_NAME)) {
            String subWorkflowCode = jobProps.getString(FLOW_NAME);
        }
        Map<String, String> inNodeResult = new HashMap<>();
        if (jobProps.containsKey(IN_NODES)) {
            String inNodes = jobProps.getString(IN_NODES);
            if (inNodes.length() > 0) {
                String[] nodes = inNodes.split(",");
                if (nodes.length > 0) {
                    for (String node : nodes) {
                        StringBuffer buffer = new StringBuffer();
                        File result = new File(tmpDir, String.format("%d-%s.properties.result", execId, node));
                        List<Map<String, String>> lines = (List<Map<String, String>>) JSONUtils.parseJSONFromFile(result);
                        if(lines.size()>0){
                            inNodeResult.putAll(lines.get(0));
                        }
                    }
                }
            }
        }
        info(String.format("参数为：workflowCode = %s - jobCode = %s", workflowCode, jobCode));
        List<DispWfJobParams> dispWfJobParamsList = dispWfJobParamsLoader.loadWfJobParams(workflowCode, jobCode);
        // 获取扩展参数
        String extra = jobProps.getString("extra", "{}");
        Map<String, String> extraMap = new HashMap<>();
        if (!StringUtils.isEmpty(extra) && extra.contains("{")) {
            info(String.format("extra：%s", extra));
            extraMap = (Map<String, String>) JSONUtils.parseJSONFromString(extra);
        } else {
            error(String.format("扩展参数格式错误!--%s", extra));
        }
        // 插入到executionParam表
        //先删除已经存在的参数
        int deleteNum = 0;
        int insertNum = 0;
        if (CollectionUtils.isNotEmpty(dispWfJobParamsList)) {
            deleteNum = dispExecutionParamsLoader.delete(execId, workflowCode, jobCode);
            insertNum = dispExecutionParamsLoader.insert(execId, workflowCode, jobCode);
        }
        info(String.format("删除了%d条数据，新增了%d数据", deleteNum, insertNum));
        //获取跟executionId绑定的参数
        List<DispExecutionParams> dispExecutionParams = dispExecutionParamsLoader.loadExecutionParams(execId, workflowCode, jobCode);
        info(String.format("查询出来的数据条数：%d", dispExecutionParams.size()));
        //加入到Map中
        Map<String, String> paramMap = new HashMap<>();
        dispExecutionParams.forEach(m -> paramMap.put(m.getParamKey(), m.getParamValue()));
        // 扩展参数中的优先级高
        paramMap.putAll(extraMap);
        // 把上个节点的数据加入到参数中
        paramMap.putAll(inNodeResult);
        List<String> contents = new ArrayList<>();
        paramMap.forEach((key, value) -> {
            if (value != null) {
                contents.add(key);
                contents.add(value);
            }
        });
        //写入到临时文件
        String fileName = String.format("%d-%s.properties", execId, jobCode);
        File tempProp = new File(tmpDir, fileName);
        tempProp.deleteOnExit();
        tempProp.createNewFile();
        FileUtils.writeLines(tempProp, contents);
        String filePath = tempProp.getPath();
        info(String.format("参数文件路径为%s", filePath));
        for (String command : commands) {
            AzkabanProcessBuilder builder = null;
            if (isExecuteAsUser) {
                //命令后面跟上参数路径
                command =
                        String.format("%s %s %s %s", executeAsUserBinaryPath, effectiveUser,
                                command, filePath);
                info("Command: " + command);
                builder =
                        new AzkabanProcessBuilder(partitionCommandLine(command))
                                .setEnv(envVars).setWorkingDir(getCwd()).setLogger(getLog())
                                .enableExecuteAsUser().setExecuteAsUserBinaryPath(executeAsUserBinaryPath)
                                .setEffectiveUser(effectiveUser);
            } else {
                //命令后面跟上参数路径
                command = String.format("%s %s", command, filePath);
                info("Command: " + command);
                builder =
                        new AzkabanProcessBuilder(partitionCommandLine(command))
                                .setEnv(envVars).setWorkingDir(getCwd()).setLogger(getLog());
            }

            if (builder.getEnv().size() > 0) {
                info("Environment variables: " + builder.getEnv());
            }
            info("Working directory: " + builder.getWorkingDir());

            // print out the Job properties to the job log.
            this.logJobProperties();

            boolean success = false;
            this.process = builder.build();

            try {
                this.process.run();
                success = true;
            } catch (Throwable e) {
                for (File file : propFiles)
                    if (file != null && file.exists())
                        file.delete();
                throw new RuntimeException(e);
            } finally {
                this.process = null;
                info("Process completed "
                        + (success ? "successfully" : "unsuccessfully") + " in "
                        + ((System.currentTimeMillis() - startMs) / 1000) + " seconds.");
            }
        }
        // Get the output properties from this job.
        generateProperties(propFiles[1]);
    }

    private boolean determineExecuteAsUser(Props sysProps, Props jobProps) {
        return sysProps.getBoolean(EXECUTE_AS_USER, false);
    }

    /**
     * <pre>
     * This method extracts the kerberos ticket cache file name from the jobprops.
     * This method will ensure that each job execution will have its own kerberos ticket cache file
     * Given that the code only sets an environmental variable, the number of files created corresponds
     * to the number of processes that are doing kinit in their flow, which should not be an inordinately
     * high number.
     * </pre>
     *
     * @return file name: the kerberos ticket cache file to use
     */
    private String getKrb5ccname(Props jobProps) {
        String effectiveUser = getEffectiveUser(jobProps);
        String projectName =
                jobProps.getString(PROJECT_NAME).replace(" ", "_");
        String flowId =
                jobProps.getString(CommonJobProperties.FLOW_ID).replace(" ", "_");
        String jobId =
                jobProps.getString(CommonJobProperties.JOB_ID).replace(" ", "_");
        // execId should be an int and should not have space in it, ever
        String execId = jobProps.getString(EXEC_ID);
        String krb5ccname =
                String.format("/tmp/krb5cc__%s__%s__%s__%s__%s", projectName, flowId,
                        jobId, execId, effectiveUser);

        return krb5ccname;
    }

    /**
     * <pre>
     * Determines what user id should the process job run as, in the following order of precedence:
     * 1. USER_TO_PROXY
     * 2. SUBMIT_USER
     * </pre>
     *
     * @param jobProps
     * @return the user that Azkaban is going to execute as
     */
    private String getEffectiveUser(Props jobProps) {
        String effectiveUser = null;
        if (jobProps.containsKey(USER_TO_PROXY)) {
            effectiveUser = jobProps.getString(USER_TO_PROXY);
        } else if (jobProps.containsKey(CommonJobProperties.SUBMIT_USER)) {
            effectiveUser = jobProps.getString(CommonJobProperties.SUBMIT_USER);
        } else {
            throw new RuntimeException(
                    "Internal Error: No user.to.proxy or submit.user in the jobProps");
        }
        info("effective user is: " + effectiveUser);
        return effectiveUser;
    }

    /**
     * This is used to get the min/max memory size requirement by processes.
     * SystemMemoryInfo can use the info to determine if the memory request can be
     * fulfilled. For Java process, this should be Xms/Xmx setting.
     *
     * @return pair of min/max memory size
     */
    protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
        return new Pair<Long, Long>(0L, 0L);
    }

    protected void handleError(String errorMsg, Exception e) throws Exception {
        error(errorMsg);
        if (e != null) {
            throw new Exception(errorMsg, e);
        } else {
            throw new Exception(errorMsg);
        }
    }

    protected List<String> getCommandList() {
        List<String> commands = new ArrayList<String>();
        commands.add(jobProps.getString(COMMAND));
        for (int i = 1; jobProps.containsKey(COMMAND + "." + i); i++) {
            commands.add(jobProps.getString(COMMAND + "." + i));
        }

        return commands;
    }

    @Override
    public void cancel() throws InterruptedException {
        if (process == null)
            throw new IllegalStateException("Not started.");
        boolean killed = process.softKill(KILL_TIME_MS, TimeUnit.MILLISECONDS);
        if (!killed) {
            warn("Kill with signal TERM failed. Killing with KILL signal.");
            process.hardKill();
        }
    }

    @Override
    public double getProgress() {
        return process != null && process.isComplete() ? 1.0 : 0.0;
    }

    public int getProcessId() {
        return process.getProcessId();
    }

    public String getPath() {
        return _jobPath == null ? "" : _jobPath;
    }

    /**
     * Splits the command into a unix like command line structure. Quotes and
     * single quotes are treated as nested strings.
     *
     * @param command
     * @return
     */
    public static String[] partitionCommandLine(final String command) {
        ArrayList<String> commands = new ArrayList<String>();

        int index = 0;

        StringBuffer buffer = new StringBuffer(command.length());

        boolean isApos = false;
        boolean isQuote = false;
        while (index < command.length()) {
            char c = command.charAt(index);

            switch (c) {
                case ' ':
                    if (!isQuote && !isApos) {
                        String arg = buffer.toString();
                        buffer = new StringBuffer(command.length() - index);
                        if (arg.length() > 0) {
                            commands.add(arg);
                        }
                    } else {
                        buffer.append(c);
                    }
                    break;
                case '\'':
                    if (!isQuote) {
                        isApos = !isApos;
                    } else {
                        buffer.append(c);
                    }
                    break;
                case '"':
                    if (!isApos) {
                        isQuote = !isQuote;
                    } else {
                        buffer.append(c);
                    }
                    break;
                default:
                    buffer.append(c);
            }

            index++;
        }

        if (buffer.length() > 0) {
            String arg = buffer.toString();
            commands.add(arg);
        }

        return commands.toArray(new String[commands.size()]);
    }
}
