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

package azkaban.executor.mail;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.utils.EmailMessage;
import azkaban.utils.Emailer;
import azkaban.utils.Utils;

public class DefaultMailCreator implements MailCreator {
  public static final String DEFAULT_MAIL_CREATOR = "default";
  private static HashMap<String, MailCreator> registeredCreators =
      new HashMap<String, MailCreator>();
  private static MailCreator defaultCreator;

  private static final DateFormat DATE_FORMATTER = new SimpleDateFormat(
      "yyyy/MM/dd HH:mm:ss z");

  public static void registerCreator(String name, MailCreator creator) {
    registeredCreators.put(name, creator);
  }

  public static MailCreator getCreator(String name) {
    MailCreator creator = registeredCreators.get(name);
    if (creator == null) {
      creator = defaultCreator;
    }
    return creator;
  }

  static {
    defaultCreator = new DefaultMailCreator();
    registerCreator(DEFAULT_MAIL_CREATOR, defaultCreator);
  }

  @Override
  public boolean createFirstErrorMessage(ExecutableFlow flow,
      EmailMessage message, String azkabanName, String scheme,
      String clientHostname, String clientPortNumber, String... vars) {

    ExecutionOptions option = flow.getExecutionOptions();
    List<String> emailList = option.getFailureEmails();
    int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow:" + flow.getFlowId() + "' has Failed");

      message.println("<h2 style=\"color:#FF0000\"> '"+getZh_CN("任务流：")+
           flow.getFlowId()+getZh_CN(" 执行编号为：")+ flow.getExecutionId()
              + getZh_CN(" 已经执行失败")+"'</h2>");

      if (option.getFailureAction() == FailureAction.CANCEL_ALL) {
        message
            .println(getZh_CN("当前流被设置为取消所有正在运行的任务！"));
      } else if (option.getFailureAction() == FailureAction.FINISH_ALL_POSSIBLE) {
        message
            .println(getZh_CN("当前流程将被设置为完成未被失败阻止的所有工作"));
      } else {
        message
            .println(getZh_CN("此流程将在停止之前完成所有当前运行的作业"));
      }

      message.println("<table>");
      message.println("<tr><td>"+getZh_CN("开始时间")+"</td><td>"
          + convertMSToString(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("结束时间")+"</td><td>"
          + convertMSToString(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("运行时长")+"</td><td>"
          + Utils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("状态")+"</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
     /** String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;
      message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId()
          + " Execution Link</a>");

      message.println("");
      message.println("<h3>Reason</h3>");
      List<String> failedJobs = Emailer.findFailedJobs(flow);
      message.println("<ul>");
      for (String jobId : failedJobs) {
        message.println("<li><a href=\"" + executionUrl + "&job=" + jobId
            + "\">Failed job '" + jobId + "' Link</a></li>");
      }

      message.println("</ul>");*/
      return true;
    }

    return false;
  }

  @Override
  public boolean createErrorEmail(ExecutableFlow flow, EmailMessage message,
      String azkabanName, String scheme, String clientHostname,
      String clientPortNumber, String... vars) {

    ExecutionOptions option = flow.getExecutionOptions();

    List<String> emailList = option.getFailureEmails();
    int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow:" + flow.getFlowId() + "' "+"has Failed");

      message.println("<h2 style=\"color:#FF0000\"> '"+getZh_CN("任务流：")+
              flow.getFlowId()+getZh_CN(" 执行编号为：")+ flow.getExecutionId()
              + getZh_CN(" 已经执行失败")+"'</h2>");
      message.println("<table>");
      message.println("<tr><td>"+getZh_CN("开始时间")+"</td><td>"
          + convertMSToString(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("结束时间")+"</td><td>"
          + convertMSToString(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>Duration</td><td>"
          + Utils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("状态")+"</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
     /* String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;
      message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId()
          + " Execution Link</a>");

      message.println("");
      message.println("<h3>Reason</h3>");
      List<String> failedJobs = Emailer.findFailedJobs(flow);
      message.println("<ul>");
      for (String jobId : failedJobs) {
        message.println("<li><a href=\"" + executionUrl + "&job=" + jobId
            + "\">Failed job '" + jobId + "' Link</a></li>");
      }
      for (String reasons : vars) {
        message.println("<li>" + reasons + "</li>");
      }

      message.println("</ul>");*/
      return true;
    }
    return false;
  }

  @Override
  public boolean createSuccessEmail(ExecutableFlow flow, EmailMessage message,
      String azkabanName, String scheme, String clientHostname,
      String clientPortNumber, String... vars) {

    ExecutionOptions option = flow.getExecutionOptions();
    List<String> emailList = option.getSuccessEmails();

    int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow:" + flow.getFlowId() + "' has Successed");
      message.println("<h2 style=\"color:#00E5EE\"> '"+getZh_CN("任务流：")+
              flow.getFlowId()+getZh_CN(" 执行编号为：")+ flow.getExecutionId()
              + getZh_CN(" 已经执行成功！")+"'</h2>");
      message.println("<table>");
      message.println("<tr><td>"+getZh_CN("开始时间")+"</td><td>"
          + convertMSToString(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("结束时间")+"</td><td>"
          + convertMSToString(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("运行时间")+"</td><td>"
          + Utils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>"+getZh_CN("状态")+"</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
     /* String executionUrl =
          scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
              + "executor?" + "execid=" + execId;
      message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId()
          + " Execution Link</a>");*/
      return true;
    }
    return false;
  }

  private static String convertMSToString(long timeInMS) {
    if (timeInMS < 0) {
      return "N/A";
    } else {
      return DATE_FORMATTER.format(new Date(timeInMS));
    }
  }
  public String getZh_CN(String str)
  {
    try {
      return new String((str).getBytes("UTF-8"), "ISO-8859-1");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return str;
  }
}
