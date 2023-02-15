package com.cqx.cli.tool.util;

import com.cqx.common.utils.file.FileCount;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.ftp.FtpParamCfg;
import com.cqx.common.utils.sftp.SftpConnection;
import com.cqx.common.utils.sftp.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * GetAvscUtils
 *
 * @author chenqixu
 */
public class GetAvscUtils {
    private static final Logger logger = LoggerFactory.getLogger(GetAvscUtils.class);

    /**
     * 从远程服务器获取avsc
     *
     * @param sftpConnection
     * @param file_path
     * @param file_name
     * @return
     */
    public String avscFromSftp(SftpConnection sftpConnection, String file_path, String file_name) {
        StringBuilder sb = new StringBuilder();
        try (InputStream inputStream = SftpUtil.ftpFileDownload(sftpConnection, file_path, file_name)) {
            //设置SFTP下载缓冲区
            byte[] buffer = new byte[2048];
            int c;
            while ((c = inputStream.read(buffer)) != -1) {
                sb.append(new String(buffer, 0, c));
            }
        } catch (IOException e) {
            logger.error("从远程服务器获取文件内容失败！");
            logger.error(e.getMessage(), e);
            return null;
        }
        return sb.toString();
    }

    /**
     * 从远程服务器获取avsc
     *
     * @param ftpParamCfg
     * @param file_path
     * @param file_name
     * @return
     */
    public String avscFromSftp(FtpParamCfg ftpParamCfg, String file_path, String file_name) {
        try (SftpConnection sftpConnection = SftpUtil.getSftpConnection(ftpParamCfg)) {
            return avscFromSftp(sftpConnection, file_path, file_name);
        } catch (IOException e) {
            logger.error("从远程服务器获取文件内容失败！");
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 从文件读取avsc
     *
     * @param file_path
     * @param file_name
     * @return
     * @throws IOException
     */
    public String avscFromFile(String file_path, String file_name) throws IOException {
        // 读取文件
        FileCount fileCount;
        FileUtil fileUtils = new FileUtil();
        final StringBuilder ogg = new StringBuilder();
        try {
            fileCount = new FileCount() {
                @Override
                public void run(String content) throws IOException {
                    ogg.append(content);
                }
            };
            fileUtils.setReader(FileUtil.endWith(file_path) + file_name);
            fileUtils.read(fileCount);
        } finally {
            fileUtils.closeRead();
        }
        return ogg.toString();
    }
}
