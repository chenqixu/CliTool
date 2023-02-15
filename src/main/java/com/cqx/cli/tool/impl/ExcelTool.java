package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.excel.ExcelSheetList;
import com.cqx.common.utils.excel.ExcelUtils;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.param.ParamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

/**
 * Excel工具
 *
 * @author chenqixu
 */
@ToolImpl
public class ExcelTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(ExcelTool.class);
    private ExcelUtils excelUtils;
    private String name;

    @Override
    public void init(Map param) throws Exception {
        name = ParamUtil.getStringVal(param, "name");
        if (!FileUtil.isFile(name)) {
            throw new FileNotFoundException(String.format("找不到文件[%s]", name));
        }
        if (name.endsWith(".xls") || name.endsWith(".xlsx")) {
        } else {
            throw new FileNotFoundException(String.format(" 不是Excel文件[%s]", name));
        }
        excelUtils = new ExcelUtils();
    }

    @Override
    public boolean execHasRet() throws Exception {
        List<ExcelSheetList> excelSheetLists = excelUtils.readExcel(name);
        for (ExcelSheetList excelSheetList : excelSheetLists) {
            logger.info("{}", excelSheetList.getSheetName());
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        // not thing to do
    }

    @Override
    public String getType() {
        return "excel_tool";
    }

    @Override
    public String getDesc() {
        return "Excel工具";
    }

    @Override
    public String getHelp() {
        return "建设中……";
    }
}
