package azkaban.wfJobParams;

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
public class JdbcDispWfJobParamsLoader extends AbstractJdbcLoader implements DispWfJobParamsLoader {
    private static Logger logger = Logger.getLogger(JdbcDispWfJobParamsLoader.class);

    public JdbcDispWfJobParamsLoader(Props props) {
        super(props);
    }

    private EncodingType defaultEncodingType = EncodingType.PLAIN;
    private static String GET_DISP_WF_JOB_PARAMS =
            "SELECT " +
                    " xdwjp.param_id, " +
                    "xdwjp.workflow_id," +
                    "xdwjp.job_id," +
                    "xdwjp.param_key," +
                    "xdwjp.param_value," +
                    "xdwjp.tenant_id," +
                    "xdwjp.graph_id " +
                    "FROM " +
                    "xdis_disp_wf_job_params AS xdwjp " +
                    "LEFT JOIN xdis_dispatch_workflow xdw ON xdwjp.workflow_id = xdw.workflow_id " +
                    "LEFT JOIN xdis_disp_workflow_job xdwj ON xdwj.source_job_id = xdwjp.job_id " +
                    "AND xdwjp.graph_id = xdwj.graph_id " +
                    "WHERE " +
                    "xdw.workflow_name = ? " +
                    "AND xdwj.workflow_job_name = ? " +
                    "AND xdwj.job_type = 'job'";


    public class DispWfJobParamsResultHandler implements ResultSetHandler<List<DispWfJobParams>> {

        @Override
        public List<DispWfJobParams> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<DispWfJobParams>emptyList();
            }

            ArrayList<DispWfJobParams> params = new ArrayList<>();
            do {
                Long paramId = rs.getLong(1);
                Long workflowId = rs.getLong(2);
                Long jobId = rs.getLong(3);
                String paramKey = rs.getString(4);
                String paramValue = rs.getString(5);
                Long tenantId = rs.getLong(6);
                String graphId = rs.getString(7);
                Object jsonObj = null;

                DispWfJobParams param = new DispWfJobParams();
                param.setParamId(paramId);
                param.setWorkflowId(workflowId);
                param.setJobId(jobId);
                param.setParamKey(paramKey);
                param.setParamValue(paramValue);
                param.setTenantId(tenantId);
                param.setGraphId(graphId);
                params.add(param);
            } while (rs.next());

            return params;
        }

    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = super.getDBConnection(false);
        } catch (Exception e) {
            e.printStackTrace();
            DbUtils.closeQuietly(connection);
        }

        return connection;
    }

    @Override
    public List<DispWfJobParams> loadWfJobParams(String workflowCode, String jobCode) {
        Connection connection = getConnection();
        QueryRunner runner = new QueryRunner();
        ResultSetHandler<List<DispWfJobParams>> handler = new DispWfJobParamsResultHandler();
        List<DispWfJobParams> params = null;
        try {
            logger.info(String.format("执行的sql为：%s参数为：%s,%s", GET_DISP_WF_JOB_PARAMS, workflowCode, jobCode));
            params = runner.query(connection, GET_DISP_WF_JOB_PARAMS, handler, workflowCode, jobCode);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(GET_DISP_WF_JOB_PARAMS + " failed.");
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return params;
    }
}
