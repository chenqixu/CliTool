@rem 设置应用路径
@set APP_HOME=%~dp0

@rem 设置参数
@set APP_MAINCLASS=com.cqx.cli.tool.ToolMain
@set APP_JAR=%APP_HOME%jarpath
@set APP_LOG=%APP_HOME%toolconfig\logback.xml

@rem 类路径
@set CLASSPATH=
@rem 设置变量延迟机制，后面的变量不能使用%xx%，而要使用!xx!，因为windows是对每一行进行预编译，并不是在执行中动态设置
@setlocal enabledelayedexpansion
@rem for循环文件夹, %%i表示变量，括号中的内容算一行，但是要加上@否则会打印括号的命令
@for %%i in (%APP_JAR%\*) do @(
	@set CLASSPATH=%%i;!CLASSPATH!
)

@rem 运行nltool
@"%JAVA_HOME%\bin\java" -Dlogback.configurationFile=%APP_LOG% -classpath %CLASSPATH% %APP_MAINCLASS% %*
