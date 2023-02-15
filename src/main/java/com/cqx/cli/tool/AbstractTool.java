package com.cqx.cli.tool;

import java.util.Map;

/**
 * AbstractTool
 *
 * @author chenqixu
 */
public abstract class AbstractTool implements ITool {

    public abstract boolean execHasRet() throws Exception;

    @Override
    public void exec() throws Exception {
        execHasRet();
    }

    protected String getStringVal(Map<?, ?> params, String name) {
        Object obj = params.get(name);
        if (obj != null) return obj.toString();
        else throw new NullPointerException(String.format("参数[%s]为空！", name));
    }
}
