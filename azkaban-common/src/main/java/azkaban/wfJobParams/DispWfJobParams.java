package azkaban.wfJobParams;


/**
 * <p>任务流任务参数表</p>
 *
 * @author zhilong.deng@hand-china.com 2019-10-11 20:15:47
 */
public class DispWfJobParams{

    //
    // 业务方法(按public protected private顺序排列)
    // ------------------------------------------------------------------------------

    //
    // 数据库字段
    // ------------------------------------------------------------------------------

    private Long paramId;

    private Long workflowId;

    private Long jobId;

    private String paramKey;

    private String paramValue;

    private Long tenantId;

    private String graphId;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(String graphId) {
        this.graphId = graphId;
    }

    public Long getParamId() {
        return paramId;
    }

    public void setParamId(Long paramId) {
        this.paramId = paramId;
    }

    public Long getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(Long workflowId) {
        this.workflowId = workflowId;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getParamKey() {
        return paramKey;
    }

    public void setParamKey(String paramKey) {
        this.paramKey = paramKey;
    }

    public String getParamValue() {
        return paramValue;
    }

    public void setParamValue(String paramValue) {
        this.paramValue = paramValue;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }
}