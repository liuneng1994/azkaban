package azkaban.executionParams;

import azkaban.wfJobParams.DispWfJobParams;

import java.util.List;

/**
 * <p>任务流任务参数表</p>
 *
 * @author zhilong.deng@hand-china.com 2019-10-11 20:15:47
 */
public interface DispExecutionParamsLoader {
    int insert(Long executionId, String workflowCode, String jobCode);

    int delete(Long executionId, String workflowCode, String jobCode);

    List<DispExecutionParams> loadExecutionParams(Long execId, String workflowCode, String jobCode);
}
