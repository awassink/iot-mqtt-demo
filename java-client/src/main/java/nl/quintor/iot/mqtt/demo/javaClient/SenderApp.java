package nl.quintor.iot.mqtt.demo.javaClient;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.text.SimpleDateFormat;

public class SenderApp {

    public static void main(String[] args) {

        String topic        = "/mqtt/topic";
        String content      = "Message from my device! I'm number: ";
        int qos             = 2;
//        String broker       = "tcp://koa-master:31883";
        String broker       = "tcp://104.154.20.191:1883";
        String clientId     = "JavaSampleSender";
        MemoryPersistence persistence = new MemoryPersistence();
        int count           = 1;

        try {
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setConnectionTimeout(5);
            connOpts.setKeepAliveInterval(10);
            for(;;) {
                try {
                    System.out.println("Connecting to broker: " + broker);
                    sampleClient.connect(connOpts);
                    System.out.println("Connected");
                    try {
                        for (int i=0; i<40; i++) {
                            String messageContent = content + count++;
                            System.out.println("Publishing message: " + messageContent);
                            MqttMessage message = new MqttMessage(messageContent.getBytes());
                            message.setQos(qos);
                            sampleClient.publish(topic, message);
                            System.out.println("Message published");
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException ie) {
                            }
                        }
                    } catch (Throwable e) {
                        System.out.println("Error " + e.getMessage());
                    }
                    sampleClient.disconnect();
                    System.out.println("Disconnected");
                }catch (Throwable e){
                    System.out.println("Error " + e.getMessage());
                }
            }
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
    }
}