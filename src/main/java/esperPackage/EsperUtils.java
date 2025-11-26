package esperPackage;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import com.rabbitmq.client.Channel;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TimeZone;

public class EsperUtils {
    private static EPRuntime epRuntime;
    private static EPCompiler epCompiler;

    private static Channel channelUCA;

    public static Configuration configuration;

    public final static String OUTPUT_QUEUE_NAME = "OutputMessages";

    public static void init() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        if (configuration == null) {
            configuration = new Configuration();
            configuration.configure("config.xml");
            configuration.getCommon().getEventMeta().getAvroSettings().setEnableAvro(false);
        }

        if (epCompiler == null) {
            epCompiler = EPCompilerProvider.getCompiler();
        }

        if (epRuntime == null) {
            epRuntime = EPRuntimeProvider.getDefaultRuntime(configuration);
        }
    }

    public static EPRuntime getEpRuntime() {
        synchronized (EsperUtils.class) {
            if (epRuntime == null) {
                throw new RuntimeException("Unable to continue because epRuntime is not defined!");
            }
        }
        return epRuntime;
    }

    public static EPCompiler getEpCompiler() {
        synchronized (EsperUtils.class) {
            if (epCompiler == null) {
                throw new RuntimeException("Unable to continue because epCompiler is not defined!");
            }
        }
        return epCompiler;
    }

    public static EPDeployment deployPattern(String epl) throws EPCompileException, EPDeployException {
        return getEpRuntime().getDeploymentService().deploy(compileNewPattern(epl));
    }

    public static void undeployAll() throws EPUndeployException {
        getEpRuntime().getDeploymentService().undeployAll();
    }

    public static EPCompiled compileNewPattern(String epl) throws EPCompileException {
        CompilerArguments arguments = new CompilerArguments(getEpRuntime().getRuntimePath());
        arguments.setConfiguration(configuration);

        return getEpCompiler().compile(epl, arguments);
    }

    public static void sendEvent(String json, String eventTypeName) {
        getEpRuntime().getEventService().sendEventJson(json, eventTypeName);
    }

    public static void sendEvent(Map<String, Object> map, String eventTypeName) {
        getEpRuntime().getEventService().sendEventMap(map, eventTypeName);
    }
    public static void addListener(EPStatement statement, Channel outputChannelUCA) {
        channelUCA = outputChannelUCA;

        statement.addListener((newComplexEvents, oldComplexEvents, detectedEventPattern, epRuntime) -> {
            if (newComplexEvents != null) {
                String eventPatternName = detectedEventPattern.getEventType().getName();
                Map<String, Object> underlying = (Map<String, Object>) newComplexEvents[0].getUnderlying();
                JSONObject out = new JSONObject(underlying);

                System.out.println("** Complex event '" + eventPatternName + "' detected: " + out);
                out.put("EventoComplejo", eventPatternName);
                try {
                    channelUCA.basicPublish("", OUTPUT_QUEUE_NAME, null, out.toString().getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
    public static void sendEventTyped(String json, String eventType) {
        getEpRuntime().getEventService().sendEventJson(json, eventType);
    }
}
