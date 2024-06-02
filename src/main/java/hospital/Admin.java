
package hospital;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;


public class Admin {
    private final static String INFO_EXCHANGE = "info";
    private final static String LOG_EXCHANGE = "log";

    private final Channel channel;
    private final Scanner sc;

    private String logQueue;

    public Admin() throws IOException, TimeoutException {
        channel = createChannel();
        declareExchanges();

        sc = new Scanner(System.in);
    }

    private Channel createChannel() throws IOException, TimeoutException {
        final Channel channel;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        return channel;
    }

    private void declareExchanges() throws IOException {
        channel.exchangeDeclare(INFO_EXCHANGE, "fanout");
        channel.exchangeDeclare(LOG_EXCHANGE, "fanout");
    }

    private void declareQueues() throws IOException {
        logQueue = channel.queueDeclare().getQueue();
    }

    private void bindQueues() throws IOException {
        channel.queueBind(logQueue, LOG_EXCHANGE, "");
    }

    private void listenQueues(String queueName, DeliverCallback deliverCallback) throws IOException {
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    private DeliverCallback setLogHandler() {

        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                handleLogs(message);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

        };

    }

    private void handleLogs(String message) throws InterruptedException {
        System.out.println(message);
    }


    public void run() throws IOException {
        declareQueues();
        bindQueues();
        DeliverCallback logCallback = setLogHandler();
        listenQueues(logQueue, logCallback);

        // main loop
        while (sc.hasNextLine()) {
            String info = sc.nextLine();


            sendInfo(channel, info);


        }

        sc.close();
    }


    private void sendInfo(Channel channel, String message) throws IOException {

        channel.basicPublish(
                INFO_EXCHANGE,
                "",
                null,
                message.getBytes(StandardCharsets.UTF_8)
        );
        System.out.println("Sent message: " + message);

    }


    public static void main(String[] args) throws IOException, TimeoutException {
        Admin admin = new Admin();
        admin.run();
    }


}
