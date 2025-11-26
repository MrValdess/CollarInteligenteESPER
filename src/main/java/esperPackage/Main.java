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
        Patron1(
                "insert into Patron1 " +
                        "select * from Temperature WHERE temp>10;"

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
        EsperUtils.deployPattern("@public @buseventtype create json schema Temperature (EvenTypeName String,temp int)");
        System.out.println("ESQUEMA");

//AÑADIMOS LOS PATRONES AL MOTOR DE EVENTOS COMPLEJOS
        EPStatement[] statements = EsperUtils.deployPattern(generatePatterns(Pattern.Patron1)).getStatements();
        for (EPStatement epStatement: statements) {
            EsperUtils.addListener(epStatement,outputChannelUCA);
        }

        System.out.println(" [*] Waiting for messages...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("RECIBIDO: "+message);
            JSONObject myJSONMessage = new JSONObject(message);

            String eventTypeName = myJSONMessage.getString("eventTypeName");
            if (eventTypeName!=null) {
                System.out.println ("TIPO: "+ eventTypeName);
                EsperUtils.sendEventTyped(myJSONMessage.toString(), eventTypeName);
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