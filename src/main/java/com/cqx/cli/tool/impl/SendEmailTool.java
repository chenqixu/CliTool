package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.email.EmailServerBean;
import com.cqx.common.utils.email.EmailUserBean;
import com.cqx.common.utils.email.EmailUtil;
import com.cqx.common.utils.file.FileCount;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.param.ParamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 邮件发送工具
 *
 * @author chenqixu
 */
@ToolImpl
public class SendEmailTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(SendEmailTool.class);
    private EmailServerBean serverBean;
    private EmailUserBean send;
    private List<EmailUserBean> receiveList = new ArrayList<>();
    private String subject;
    private String content;
    private List<String> appendixs;
    private EmailUtil emailUtil;

    @Override
    public void init(Map param) throws Exception {
        // email_server 服务器
        Map email_server = (Map) param.get("email_server");
        String host = getStringVal(email_server, "host");
        String port = getStringVal(email_server, "port");
        String protocol = getStringVal(email_server, "protocol");
        boolean is_ssl = ParamUtil.setValDefault(email_server, "is_ssl", true);
        serverBean = new EmailServerBean(host, port, protocol, is_ssl);

        // send_account 发送者
        Map send_account = (Map) param.get("send_account");
        String account = getStringVal(send_account, "account");
        String password = getStringVal(send_account, "password");
        send = new EmailUserBean(account, password);

        // receive_account 接收者
        List<String> receive_account = (List<String>) param.get("receive_account");
        for (String _receive_account : receive_account) {
            EmailUserBean receive = new EmailUserBean(_receive_account);
            receiveList.add(receive);
        }

        // subject 主题
        subject = getStringVal(param, "subject");

        // content 正文
        content = getStringVal(param, "content");
        // 如果是文件，需要读取文件内容
        File file = new File(content);
        if (file.isFile()) {
            logger.info("正文 {} 是一个文件，读取正文的内容", content);
            StringBuilder sb = new StringBuilder();
            FileUtil fileUtils = new FileUtil();
            try {
                fileUtils.setReader(content);
                fileUtils.read(new FileCount() {
                    @Override
                    public void run(String content) throws IOException {
                        sb.append(content).append("<br/>");
                    }
                });
                content = sb.toString();
            } finally {
                fileUtils.closeRead();
            }
        }

        // appendixs 附件
        appendixs = (List<String>) param.get("appendixs");

        // 工具
        emailUtil = new EmailUtil(ParamUtil.setValDefault(param, "is_debug", false));

        logger.info("【参数打印 | 开始】===================");
        logger.info("【邮件服务器 | host】{}", host);
        logger.info("【邮件服务器 | port】{}", port);
        logger.info("【邮件服务器 | protocol】{}", protocol);
        logger.info("【邮件服务器 | is_ssl】{}", is_ssl);
        logger.info("【发送对象 | account】{}", account);
        logger.info("【发送对象 | password】{}", password);
        for (EmailUserBean _receive : receiveList) {
            logger.info("【接收对象 | account】{}", _receive.getAccount());
        }
        logger.info("【标题 | subject】{}", subject);
        logger.info("【正文 | content】{}", content);
        for (String _appendix : appendixs) {
            logger.info("【附件 | appendix】{}", _appendix);
        }
        logger.info("【参数打印 | 完成】===================");
    }

    @Override
    public boolean execHasRet() throws Exception {
        // 发送邮件
        emailUtil.sendMail(serverBean, send, receiveList, subject, content, appendixs);
        return true;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public String getType() {
        return "send_email";
    }

    @Override
    public String getDesc() {
        return "发送邮件";
    }

    @Override
    public String getHelp() {
        return "建设中……";
    }
}
