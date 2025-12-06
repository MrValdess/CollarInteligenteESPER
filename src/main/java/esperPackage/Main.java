package esperPackage;

import com.espertech.esper.runtime.client.EPStatement;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import org.json.JSONObject;

public class Main {

    final static String INPUT_QUEUE_NAME = "InputMessages";

    //Patterns definition
    enum Pattern {
        AlertaPulsaciones(
                "@public insert into AlertaPulsaciones " +
                        "select  p1.idcollar as idcollar, p1.nombre as NomMascota, p1.valor as Pul1, p2.valor as Pul2 " +
                        " from pattern [every ( p1 = Pulsaciones(p1.valor > PULMAX or p1.valor < PULMIN) or (p1 = Pulsaciones() -> p2 = Pulsaciones(Math.abs(p2.valor - p1.valor) >= 40 and p1.idcollar = p2.idcollar))) where timer:within(15 seconds)];"
        ),
        AlertaTemperatura(
                "@public insert into AlertaTemperatura " +
                        "select  t1.idcollar as idcollar, t1.nombre as NomMascota, t1.valor as Temp1, t2.valor as Temp2" +
                        " from pattern [every ( t1 = Temperatura(t1.valor > TEMPMAX or t1.valor < TEMPMIN) or (t1= Temperatura() -> t2 = Temperatura(Math.abs(t2.valor - t1.valor) >= 1.5 and t1.idcollar = t2.idcollar))) where timer:within(15 seconds)];"
        ),
        ZonaDesconocida(
                "@public insert into ZonaDesconocida " +
                        "select * " +
                        "from pattern [every l = Localizacion (permitida = 'No')];"
        ),
        AlertaRespiraciones(
                "@public insert into AlertaRespiraciones " +
                        "select r1.idcollar as idcollar, r1.nombre as NomMascota, r1.valor as Resp1, r2.valor as Resp2" +
                        " from pattern [every ( r1 = Respiraciones(r1.valor > RESPMAX or r1.valor < RESPMIN) or (r1=Respiraciones() -> r2 = Respiraciones(Math.abs(r2.valor - r1.valor) >= 25 and r1.idcollar = r2.idcollar))) where timer:within(15 seconds)];"
        ),
        ComederoVacio(
                "@public insert into ComederoVacio " +
                        "select c.idcomedero" +
                        "from pattern [every c = Comedero (estado = 'vacio' and reserva = 'no')];"
        );

        private final String pattern;

        Pattern(String pattern) {
            this.pattern = pattern;
        }

        public String getPattern() {
            return pattern;
        }
    }

    public static void main(String[] args) throws Exception {
        Main test = new Main();
        test.run();

    }
    public void run () throws Exception
    {

        System.out.println("connected to rabbitMQ as CONSUMER...");
        //Stablish inputconnection
        ConnectionFactory inputfactory = new ConnectionFactory();
        inputfactory.setHost("localhost");
        inputfactory.setUsername("guest");
        inputfactory.setPassword("guest");

        com.rabbitmq.client.Connection inputConnection = inputfactory.newConnection();
        Channel inputChannel = inputConnection.createChannel();

        inputChannel.exchangeDeclare(INPUT_QUEUE_NAME, "fanout");
        String inputQueueName = inputChannel.queueDeclare().getQueue();
        inputChannel.queueBind(inputQueueName, INPUT_QUEUE_NAME, "");

        //Stablish outputconnection UCA
        System.out.println("connected to rabbitMQ as PRODUCER...");
        ConnectionFactory outputFactoryUCA = new ConnectionFactory();
        outputFactoryUCA.setHost("localhost");
        outputFactoryUCA.setUsername("guest");
        outputFactoryUCA.setPassword("guest");

        com.rabbitmq.client.Connection outputConnectionUCA = outputFactoryUCA.newConnection();
        Channel outputChannelUCA = outputConnectionUCA.createChannel();

        outputChannelUCA.queueDeclare(EsperUtils.OUTPUT_QUEUE_NAME, false, false, false, null);

        EsperUtils.init();
        System.out.println("UTILS");

//DEFINIMOS EL ESQUEMA
        EsperUtils.deployPattern("@public @buseventtype create json schema Localizacion(idcollar int, nombre String, area String, permitida String)");
        EsperUtils.deployPattern("@public @buseventtype create json schema Pulsaciones(idcollar int, nombre String, valor int, estado String)");
        EsperUtils.deployPattern("@public @buseventtype create json schema Temperatura(idcollar int, nombre String, valor int)");
        EsperUtils.deployPattern("@public @buseventtype create json schema Respiraciones(idcollar int, nombre String, valor int, estado String)");
        EsperUtils.deployPattern("@public @buseventtype create json schema Comedero(idcomedero int, idcollar int, estado String, reserva String)");

        EsperUtils.deployPattern("@public create constant variable integer PULMAX = 150;");
        EsperUtils.deployPattern("@public create constant variable integer PULMIN = 60;");

        EsperUtils.deployPattern("@public create constant variable integer TEMPMAX = 150;");
        EsperUtils.deployPattern("@public create constant variable integer TEMPMIN = 60;");

        EsperUtils.deployPattern("@public create constant variable integer RESPMAX = 40;");
        EsperUtils.deployPattern("@public create constant variable integer RESPMIN = 10;");
        System.out.println("ESQUEMA");

//AÑADIMOS LOS PATRONES AL MOTOR DE EVENTOS COMPLEJOS
        EPStatement[] statements = EsperUtils.deployPattern(generatePatterns(Pattern.AlertaPulsaciones,
                Pattern.ZonaDesconocida, Pattern.AlertaTemperatura, Pattern.AlertaRespiraciones,
                Pattern.ComederoVacio)).getStatements();

        for (EPStatement epStatement: statements) {
            EsperUtils.addListener(epStatement,outputChannelUCA);
        }

        System.out.println(" [*] Waiting for messages...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("RECIBIDO: "+message);
            JSONObject myJSONMessage = new JSONObject(message);

            String eventTypeName = myJSONMessage.getString("schema");
            if (eventTypeName!=null) {
                System.out.println ("TIPO: "+ eventTypeName);
                EsperUtils.sendEventTyped(myJSONMessage.toString(), eventTypeName);
                System.out.println ("\n");
            }
        };

        inputChannel.basicConsume(INPUT_QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    }

    public static String generatePatterns(Pattern... patterns) {
        StringBuilder patternsBuilder = new StringBuilder();

        for (Pattern pattern: patterns) {
            System.out.println(pattern.getPattern());
            patternsBuilder.append(pattern.getPattern()).append(" ");
        }

        return patternsBuilder.toString();
    }

}