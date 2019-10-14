package azkaban.wfJobParams;

import java.util.List;

/**
 * <p>任务流任务参数表</p>
 *
 * @author zhilong.deng@hand-china.com 2019-10-11 20:15:47
 */
public interface DispWfJobParamsLoader {
    List<DispWfJobParams> loadWfJobParams(String workflowCode, String jobCode);

}
