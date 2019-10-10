package azkaban.webapp.filter;
import org.apache.log4j.Logger;
import org.mortbay.jetty.HttpMethods;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class CustomRequestFilter implements Filter {
    Logger logger = Logger
            .getLogger(CustomRequestFilter.class);
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        String method = ((HttpServletRequest)request).getMethod();
        if(!HttpMethods.TRACE.equalsIgnoreCase(method)){
            logger.info("进入正常请求");
            chain.doFilter(request, response);
        } else {
            logger.info("进入TRACE请求");
            ((HttpServletResponse)response).setStatus(400);
            return;
        }
    }

    @Override
    public void destroy() {

    }
}
