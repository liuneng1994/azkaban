package azkaban.executionParams;

import azkaban.database.AbstractJdbcLoader;
import azkaban.utils.Props;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>任务流任务参数表</p>
 *
 * @author zhilong.deng@hand-china.com 2019-10-11 20:15:47
 */
public class JdbcDispExecutionParamsLoader extends AbstractJdbcLoader implements DispExecutionParamsLoader {
    private static Logger logger = Logger.getLogger(JdbcDispExecutionParamsLoader.class);

    public JdbcDispExecutionParamsLoader(Props props) {
        super(props);
    }

    private EncodingType defaultEncodingType = EncodingType.PLAIN;
    private static String INSERT_DISP_WF_JOB_PARAMS =
            "INSERT INTO `hdsp_dispatch`.`xdis_disp_execution_params` (execution_id, workflow_id, job_id, " +
                    " param_key, param_value, tenant_id) " +
                    "SELECT ? as execution_id, xdwjp.workflow_id as workflow_id, " +
                    " xdwjp.job_id as job_id, xdwjp.param_key as param_key, xdwjp.param_value as param_value, " +
                    "xdwjp.tenant_id as tenant_id " +
                    "FROM xdis_disp_wf_job_params AS xdwjp WHERE " +
                    " xdwjp.workflow_id = (select workflow_id from xdis_dispatch_workflow where workflow_code = ?) " +
                    " AND xdwjp.job_id = (select job_id from xdis_dispatch_job where job_code = ?)";
    private static String DELETE_DISP_WF_JOB_PARAMS =
            "delete from `hdsp_dispatch`.`xdis_disp_execution_params` where " +
                    " execution_id = ? " +
                    " and workflow_id = (select workflow_id from xdis_dispatch_workflow where workflow_code = ?) " +
                    " and job_id = (select job_id from xdis_dispatch_job where job_code = ?)";
    private static String GET_DISP_EXECUTION__PARAMS =
            "SELECT xdep.param_id,xdep.execution_id, xdep.workflow_id, xdep.job_id, xdep.param_key," +
                    " xdep.param_value, xdep.tenant_id FROM xdis_disp_execution_params AS xdep WHERE " +
                    "xdep.execution_id = ? AND  xdep.workflow_id " +
                    "= (select workflow_id from xdis_dispatch_workflow where workflow_code = ?) AND xdep.job_id " +
                    "= (select job_id from xdis_dispatch_job where job_code = ?)";

    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = super.getDBConnection(false);
        } catch (Exception e) {
            DbUtils.closeQuietly(connection);
        }

        return connection;
    }

    @Override
    public int insert(Long executionId, String workflowCode, String jobCode) {
        Connection connection = getConnection();
        QueryRunner runner = super.createQueryRunner();
        int i = 0;
        try {
            logger.info(String.format("执行的sql为：%s参数为：%d,%s,%s", INSERT_DISP_WF_JOB_PARAMS, executionId, workflowCode, jobCode));
            i = runner.update(
                    INSERT_DISP_WF_JOB_PARAMS,
                    executionId,
                    workflowCode,
                    jobCode);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(INSERT_DISP_WF_JOB_PARAMS + " failed.");
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return i;
    }

    @Override
    public int delete(Long executionId, String workflowCode, String jobCode) {
        Connection connection = getConnection();
        QueryRunner runner = new QueryRunner();
        int i = 0;
        try {
            logger.info(String.format("执行的sql为：%s参数为：%d - %s - %s", DELETE_DISP_WF_JOB_PARAMS, executionId, workflowCode, jobCode));
            i = runner.update(connection,
                    DELETE_DISP_WF_JOB_PARAMS,
                    executionId, workflowCode, jobCode);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(DELETE_DISP_WF_JOB_PARAMS + " failed.");
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return i;
    }

    @Override
    public List<DispExecutionParams> loadExecutionParams(Long execId, String workflowCode, String jobCode) {
        Connection connection = getConnection();
        QueryRunner runner = new QueryRunner();
        ResultSetHandler<List<DispExecutionParams>> handler = new DispExecutionParamsResultHandler();
        List<DispExecutionParams> params = null;
        try {
            logger.info(String.format("执行的sql为：%s参数为：%d,%s,%s", GET_DISP_EXECUTION__PARAMS, execId, workflowCode, jobCode));
            params = runner.query(connection, GET_DISP_EXECUTION__PARAMS, handler, execId, workflowCode, jobCode);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(GET_DISP_EXECUTION__PARAMS + " failed.");
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return params;
    }

    public class DispExecutionParamsResultHandler implements ResultSetHandler<List<DispExecutionParams>> {

        @Override
        public List<DispExecutionParams> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<DispExecutionParams>emptyList();
            }

            List<DispExecutionParams> params = new ArrayList<>();
            do {
                Long paramId = rs.getLong(1);
                Long executionId = rs.getLong(2);
                Long workflowId = rs.getLong(3);
                Long jobId = rs.getLong(4);
                String paramKey = rs.getString(5);
                String paramValue = rs.getString(6);
                Long tenantId = rs.getLong(7);
                DispExecutionParams param = new DispExecutionParams();
                param.setParamId(paramId);
                param.setExecutionId(executionId);
                param.setWorkflowId(workflowId);
                param.setJobId(jobId);
                param.setParamKey(paramKey);
                param.setParamValue(paramValue);
                param.setTenantId(tenantId);
                params.add(param);
            } while (rs.next());

            return params;
        }

    }
}
